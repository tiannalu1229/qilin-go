package sql

import (
	"database/sql"
	"fmt"
	"github.com/tiannalu1229/qilin-go/util"
	"log"
)

var db *sql.DB

func ConnectRinkeby() *sql.DB {
	config, err := util.GetPGConfig()
	pg := config.PG
	// 链接PostgreSQL数据库
	log.Println("Connecting PostgreSQL....")

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", pg.PgHost, pg.PgPort, pg.PgUser, pg.PgPassword, pg.PgDbname)
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("Connect PG Failed: ", err)
	}

	db.SetMaxOpenConns(2000)
	db.SetMaxIdleConns(1000)

	return db
}
