package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const HASH_REDIS = "votos:total"
const HASH_REDIS_CANDIDATO = "candidatos"

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func main() {

	context := context.Background()

	cluster := gocql.NewCluster("db")
	cluster.Keyspace = "votacao"
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()

	if err != nil {
		panic(err)
	}

	defer session.Close()

	fmt.Println("CONECTOU AO CASSANDRA")

	rdb := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "r478k",
		Protocol: 2,
	})

	createKeySpace := `CREATE KEYSPACE IF NOT EXISTS votacao WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1};`

	if err := session.Query(createKeySpace).Exec(); err != nil {
		panic(err)
	}

	createTableCanditato := `CREATE TABLE IF NOT EXISTS candidato(
	id uuid,
	nome text,
	PRIMARY KEY(id));`

	createTableVoto := `CREATE TABLE IF NOT EXISTS voto(
	id uuid, 
	id_candidato uuid, 
	hora text,
	PRIMARY KEY((id_candidato), hora))`

	if err := session.Query(createTableCanditato).Exec(); err != nil {
		panic(err)
	}

	if err := session.Query(createTableVoto).Exec(); err != nil {
		panic(err)
	}

	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	router.POST("/candidatos", func(ctx *gin.Context) {
		if err := insertCandidatoHandler(context, ctx, session, rdb); err != nil {
			fmt.Println(err)
		}
	})

	router.GET("/candidatos", func(ctx *gin.Context) {
		if err := getAllCandidatoHandler(ctx, session); err != nil {
			fmt.Println(err)
		}
	})

	router.GET("/candidatos/:id", func(ctx *gin.Context) {
		if err := getCandidatoByIDHandler(ctx, session); err != nil {
			fmt.Println(err)
		}
	})

	router.POST("/votos/", func(ctx *gin.Context) {
		if err := insertVoto(context, ctx, rdb); err != nil {
			fmt.Println(err)
		}
	})

	router.GET("/votos/", func(ctx *gin.Context) {
		WebSocketVotosHandler(ctx, rdb)
	})

	go popVoto(context, rdb, session)
	router.Run()

}

type CandidatoRequest struct {
	Nome string `json:"nome"`
}

type CandidatoResponse struct {
	ID   gocql.UUID `json:"id"`
	Nome string     `json:"nome"`
}

type VotoRequest struct {
	IDCandidato gocql.UUID `json:"candidato_id"`
}

type Voto struct {
	IDCandidato gocql.UUID `json:"candidato_id"`
	IDVoto      gocql.UUID `json:"id_voto"`
	Hora        string     `json:"hora"`
}

func insertCandidatoHandler(ctx context.Context, c *gin.Context, session *gocql.Session, redisClient *redis.Client) error {

	var candidato CandidatoRequest

	if err := c.ShouldBindJSON(&candidato); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":      http.StatusBadRequest,
			"error":     "BAD_REQUEST",
			"timestamp": time.Now().String(),
		})

		return nil
	}

	uuid, err := gocql.RandomUUID()

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":      http.StatusInternalServerError,
			"error":     "INTERNAL_SERVER_ERROR",
			"timestamp": time.Now().String(),
		})
		return err
	}

	candidatoCreate := CandidatoResponse{
		ID:   uuid,
		Nome: candidato.Nome,
	}

	if err := session.Query(`INSERT INTO votacao.candidato(id, nome) VALUES(?, ?);`, candidatoCreate.ID, candidatoCreate.Nome).Exec(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":      http.StatusInternalServerError,
			"error":     "INTERNAL_SERVER_ERROR",
			"timestamp": time.Now().String(),
		})
		return err
	}

	if err := setCandidatoRedis(ctx, redisClient, candidatoCreate.ID.String(), candidatoCreate); err != nil {
		return err
	}

	c.Status(http.StatusOK)

	return nil

}

func setCandidatoRedis(ctx context.Context, redisClient *redis.Client, candidatoID string, candidato interface{}) error {

	data, err := json.Marshal(candidato)

	if err != nil {
		return err
	}

	if _, err := redisClient.HSet(ctx, HASH_REDIS_CANDIDATO, candidatoID, data).Result(); err != nil {
		fmt.Printf("erro ao setar usuario no redis %v", err)
		return err
	}

	return nil

}

func getAllCandidatoHandler(c *gin.Context, session *gocql.Session) error {

	var candidatoResponse []CandidatoResponse

	var candidato CandidatoResponse

	iter := session.Query(`SELECT * FROM votacao.candidato`).Iter()

	for iter.Scan(&candidato.ID, &candidato.Nome) {
		candidatoResponse = append(candidatoResponse, candidato)
	}

	c.JSON(http.StatusOK, candidatoResponse)

	return nil

}

func getCandidatoByIDHandler(c *gin.Context, session *gocql.Session) error {

	id := c.Param("id")

	var candidato CandidatoResponse

	if err := session.Query("SELECT * FROM votacao.candidato WHERE id = ?", id).Scan(&candidato.ID, &candidato.Nome); err != nil {
		if err.Error() == "not found" {
			c.JSON(http.StatusNotFound, gin.H{
				"code":      http.StatusNotFound,
				"error":     "STATUS_NOT_FOUND",
				"timestamp": time.Now().String()})
			return err
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{
				"code":      http.StatusInternalServerError,
				"error":     "INTERNAL_SERVER_ERROR",
				"timestamp": time.Now().String(),
			})

			return err
		}
	}

	c.JSON(http.StatusOK, candidato)

	return nil

}

func insertVoto(c context.Context, ctx *gin.Context, redisClient *redis.Client) error {

	var voto VotoRequest

	if err := ctx.ShouldBindJSON(&voto); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":      http.StatusBadRequest,
			"error":     "BAD_REQUEST",
			"timestamp": time.Now().String(),
		})

		return nil
	}

	idVoto, err := gocql.RandomUUID()

	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"code":      http.StatusInternalServerError,
			"error":     "INTERNAL_SERVER_ERROR",
			"timestamp": time.Now().String(),
		})
		return err
	}
	hora := time.Now().Format("2025-12-18-12")

	votoPublish := Voto{
		IDCandidato: voto.IDCandidato,
		IDVoto:      idVoto,
		Hora:        hora,
	}

	if err := publishVotoStream(c, votoPublish, redisClient); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"code":      http.StatusInternalServerError,
			"error":     "INTERNAL_SERVER_ERROR",
			"timestamp": time.Now().String(),
		})

		return err
	}

	ctx.Status(http.StatusOK)

	return nil
}

func saveVoto(voto Voto, session *gocql.Session) error {

	if err := session.Query("INSERT INTO votacao.voto(id, id_candidato, hora) VALUES(?, ?, ?)", voto.IDVoto, voto.IDCandidato, voto.Hora).Exec(); err != nil {
		return err
	}

	return nil

}
func publishVotoStream(ctx context.Context, voto Voto, redisClient *redis.Client) error {

	_, err := redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "voto_stream",
		Values: map[string]interface{}{
			"candidato_id": voto.IDCandidato.String(),
			"id_voto":      voto.IDVoto.String(),
			"hora":         voto.Hora,
		},
	}).Result()

	if err != nil {
		fmt.Println("erro ao publicar ")
		return err
	}

	if err := incrementarVotosRedis(ctx, redisClient, voto.IDCandidato.String()); err != nil {
		return err
	}

	return nil
}
func popVoto(ctx context.Context, redisClient *redis.Client, session *gocql.Session) {

	_, err := redisClient.XGroupCreateMkStream(ctx, "voto_stream", "voto_group", "0").Result()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return
	}

	consumer := "consumer-votos" + time.Now().String()[:2]

	for {

		pendings, err := redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
			Group:  "voto_group",
			Count:  5,
			Start:  "-",
			End:    "+",
			Stream: "voto_stream",
		}).Result()

		if err != nil {
			fmt.Println("error read messages stream ", err)
			continue
		}

		for _, pending := range pendings {
			messages, err := redisClient.XClaim(ctx, &redis.XClaimArgs{
				Stream:   "voto_stream",
				Group:    "voto_group",
				Messages: []string{pending.ID},
				Consumer: consumer,
				MinIdle:  5 * time.Minute,
			}).Result()

			if err != nil {
				fmt.Println("error claiming pending messages ", err)
				continue
			}

			for _, message := range messages {

				candidatoIDStr, ok1 := message.Values["candidato_id"].(string)
				votoIDStr, ok2 := message.Values["id_voto"].(string)
				horaStr, ok3 := message.Values["hora"].(string)

				if !ok1 || !ok2 || !ok3 {
					fmt.Println("erro no tipo ou dados faltando", ok1, ok2, ok3)
					continue
				}

				candidatoUuid, err := gocql.ParseUUID(candidatoIDStr)
				if err != nil {
					fmt.Println("erro ao fazer parse do uuid do candidato ", err)
					continue
				}

				votoUuid, err := gocql.ParseUUID(votoIDStr)
				if err != nil {
					fmt.Println("erro ao fazer parse do uuid do voto ", err)
					continue
				}

				voto := Voto{
					IDCandidato: candidatoUuid,
					IDVoto:      votoUuid,
					Hora:        horaStr,
				}

				if err := saveVoto(voto, session); err != nil {
					fmt.Println(err.Error())
					continue
				}

				redisClient.XAck(ctx, "voto_stream", "voto_group", message.ID)
			}
		}

		streamNewMessages, err := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "voto_group",
			Consumer: consumer,
			Streams:  []string{"voto_stream", ">"},
			Count:    1,
		}).Result()

		if err != nil {
			fmt.Println("error read messages stream ", err)
			continue
		}

		for _, stream := range streamNewMessages {
			for _, message := range stream.Messages {

				candidatoIDStr, ok1 := message.Values["candidato_id"].(string)
				votoIDStr, ok2 := message.Values["id_voto"].(string)
				horaStr, ok3 := message.Values["hora"].(string)

				if !ok1 || !ok2 || !ok3 {
					fmt.Println("erro no tipo ou dados faltando")
					continue
				}

				candidatoUuid, err := gocql.ParseUUID(candidatoIDStr)
				if err != nil {
					fmt.Println("erro ao fazer parse do uuid do candidato ", err)
					continue
				}

				votoUuid, err := gocql.ParseUUID(votoIDStr)
				if err != nil {
					fmt.Println("erro ao fazer parse do uuid do voto ", err)
					continue
				}

				voto := Voto{
					IDCandidato: candidatoUuid,
					IDVoto:      votoUuid,
					Hora:        horaStr,
				}

				if err := saveVoto(voto, session); err != nil {
					fmt.Println(err.Error())
					continue
				}

				redisClient.XAck(ctx, "voto_stream", "voto_group", message.ID)
			}
		}
	}
}

func incrementarVotosRedis(ctx context.Context, redisClient *redis.Client, candidatoID string) error {

	if _, err := redisClient.HIncrBy(ctx, HASH_REDIS, candidatoID, 1).Result(); err != nil {
		fmt.Printf("erro ao incrementar voto do candidado %s: %v", candidatoID, err)
		return err
	}

	data, err := obterTotalDeVotos(ctx, redisClient)

	if err != nil {
		return err
	}

	votosAtualizados, err := json.Marshal(data)

	if err != nil {
		return err
	}

	if err := redisClient.Publish(ctx, "votacao_atualizada", votosAtualizados).Err(); err != nil {
		return err
	}

	return nil

}

func obterTotalDeVotos(ctx context.Context, redisClient *redis.Client) (map[string]int64, error) {

	votos := make(map[string]int64)

	results, err := redisClient.HGetAll(ctx, HASH_REDIS).Result()

	if err != nil {
		fmt.Println("erro ao buscar total de votos dos candidatos")
		return nil, err
	}

	for k, v := range results {
		number, err := strconv.Atoi(v)

		if err != nil {
			fmt.Println("erro ao converter string em inteiro")
			return nil, err
		}

		candidato, err := redisClient.HGet(ctx, HASH_REDIS_CANDIDATO, k).Result()
		if err != nil {
			continue
		}

		var candidatoFormated CandidatoResponse

		if err := json.Unmarshal([]byte(candidato), &candidatoFormated); err != nil {
			fmt.Println("deu erro na hora de fazer o unmarshal")
			return nil, err
		}

		votos[candidatoFormated.Nome] = int64(number)

	}

	return votos, nil
}

func ObterTotalDeVotosHandler(ctx context.Context, c *gin.Context, redisClient *redis.Client) {

	votos, err := obterTotalDeVotos(ctx, redisClient)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":      http.StatusInternalServerError,
			"error":     "INTERNAL_SERVER_ERROR",
			"timestamp": time.Now().String(),
		})

		fmt.Println(err)
		return
	}

	c.JSON(http.StatusOK, votos)
}

func WebSocketVotosHandler(c *gin.Context, redis *redis.Client) {
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	pubsub := redis.Subscribe(c, "votacao_atualizada")
	defer pubsub.Close()

	for {
		msg, err := pubsub.ReceiveMessage(c)
		if err != nil {
			return
		}

		ws.WriteMessage(websocket.TextMessage, []byte(msg.Payload))
	}
}
