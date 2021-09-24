package qilin_request

import (
	"database/sql"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	_ "github.com/lib/pq"
	"github.com/tiannalu1229/qilin-go/util"
	"log"
	"strconv"
)

var db *sql.DB
var err error

type Condition struct {
	Keyword string `json:"keyword"`
	Page    int    `json:"index"`
	Size    int    `json:"page_size"`
}

type Result struct {
	Page  int         `json:"index"`
	List  []TradePair `json:"list"`
	Size  int         `json:"page_size"`
	Total int         `json:"total"`
}

type TradePair struct {
	Pool        string `json:"pool"`
	Fee         string `json:"fee"`
	PoolAddress string `json:"pool_address"`
	PoolSymbol  string `json:"pool_symbol"`
	Price       string `json:"price"`
	Tvl         string `json:"tvl"`
	Volume24h   string `json:"volume_24h"`
}

func SelectTradePair(keyword string, page int, size int) Result {

	config, err := util.GetPGConfig()
	pg := config.PG

	pgPort, _ := strconv.Atoi(pg.PgPort)
	// 链接PostgreSQL数据库
	log.Println("Connecting PostgreSQL....")

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", pg.PgHost, pgPort, pg.PgUser, pg.PgPassword, pg.PgDbname)
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("Connect PG Failed: ", err)
	}

	db.SetMaxOpenConns(2000)
	db.SetMaxIdleConns(1000)

	//select total
	num, err := db.Query("SELECT count(*) FROM t_pools where pool_symbol = '" +
		keyword + "' or trade_symbol = '" +
		keyword + "' or pool_address = '" +
		keyword + "'")

	var count int
	for num.Next() {
		if err = num.Scan(&count); err != nil {
			log.Fatal("PG Rows Scan Failed: ", err)
		}
		fmt.Println(count)
	}

	rows, err := db.Query("SELECT pl.trade_pair, " +
		"pl.fee, " +
		"to_number(pl.liquidity_pool,'9999999999999999999') + to_number(pl.total_size_short, '9999999999999999999') + to_number(pl.total_size_long, '9999999999999999999') tvl, " +
		"COALESCE(pos.size, 0) * to_number(ttp.price, '99999999999999999') volume24h, " +
		"ttp.price price, " +
		"pl.pool_symbol, " +
		"pl.pool_address " +
		"FROM t_pools pl left join " +
		"(select a.pool_address, (COALESCE(a.size, 0) + COALESCE(b.size, 0)) size from " +
		"(select pool_address, COALESCE(sum(to_number(size, '9999999999999999999')), 0) size from t_positions " +
		"where " +
		"(open_block_height >= (select to_number((select max(block_height) block_height from t_settings), '99999999999') - 5760) " +
		"and close_block_height < (select to_number((select max(block_height) block_height from t_settings), '99999999999') - 5760)) " +
		"or (open_block_height < (select to_number((select max(block_height) block_height from t_settings), '99999999999') - 5760) " +
		"and close_block_height >= (select to_number((select max(block_height) block_height from t_settings), '99999999999') - 5760)) " +
		"group by pool_address " +
		") a left join " +
		"(select pool_address, COALESCE(sum(to_number(size, '9999999999999999999')), 0) * 2 size from t_positions " +
		"where " +
		"(open_block_height >= (select to_number((select max(block_height) block_height from t_settings), '99999999999') - 5760) " +
		"and close_block_height >= (select to_number((select max(block_height) block_height from t_settings), '99999999999') - 5760)) " +
		"group by pool_address " +
		") b " +
		"on a.pool_address = b.pool_address) pos " +
		"on pos.pool_address = pl.pool_address " +
		"left join " +
		"(select ttpc.pool_address, ttpc.price from (select pool_address, max(block_height) block_height from t_trade_token_price_change group by pool_address) pb " +
		"left join t_trade_token_price_change ttpc " +
		"on ttpc.pool_address = pb.pool_address " +
		"and ttpc.block_height = pb.block_height) ttp " +
		"on pl.pool_address = ttp.pool_address " +
		"where pl.pool_symbol = '" +
		keyword + "' or pl.trade_symbol = '" +
		keyword + "' or pl.pool_address = '" +
		keyword + "' order by tvl desc limit " + strconv.Itoa(size) + " offset " + strconv.Itoa(page))

	if err != nil {
		log.Fatal("PG Statements Wrong: ", err)
	}

	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Ping GP Failed: ", err)
	}
	fmt.Println("PG Successfull Connected!")

	var pairs = make([]TradePair, 0, 1000)
	var i int = 0
	for rows.Next() {

		var tradePair string
		var fee string
		var tvl string
		var volume24h string
		var price string
		var poolSymbol string
		var poolAddress string

		if err := rows.Scan(&tradePair, &fee, &tvl, &volume24h, &price, &poolSymbol, &poolAddress); err != nil {
			log.Fatal("PG Rows Scan Failed: ", err)
		}

		pair := TradePair{
			Pool:        tradePair,
			Fee:         fee,
			PoolAddress: poolAddress,
			PoolSymbol:  poolSymbol,
			Price:       price,
			Tvl:         tvl,
			Volume24h:   volume24h,
		}
		pairs = append(pairs, pair)

		i++
	}

	result := Result{
		Page:  page,
		List:  pairs,
		Size:  size,
		Total: count,
	}

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	jsonResult, _ := json.Marshal(result)

	if err := rows.Err(); err != nil {
		log.Fatal("PG Query Failed: ", err)
	}

	rows.Close()
	db.Close()

	fmt.Println(string(jsonResult))
	return result
}
