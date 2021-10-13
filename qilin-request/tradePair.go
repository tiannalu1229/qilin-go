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
	PoolDecimal string `json:"pool_decimal"`
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
	totalSql := "SELECT count(*) FROM t_pools"

	maxHeightSql := "select (to_number(max(block_height), '99999999999') - 5760) block_height from t_settings"

	maxHeightRows, err := db.Query(maxHeightSql)

	var blockHeight string
	for maxHeightRows.Next() {
		if err = maxHeightRows.Scan(&blockHeight); err != nil {
			log.Fatal("PG Rows Scan Failed: ", err)
		}
	}

	poolSql := "select tvl.pool_address, tvl.trade_pair, tvl.pool_symbol, tvl.fee, ttpc.price, tvl.tvl, v24.sum_size_24h, tvl.pool_decimal from (SELECT t_pools.trade_pair, " +
		"t_pools.fee, t_pools.pool_symbol, t_pools.pool_decimal, t_pools.trade_symbol, " +
		"(to_number(t_pools.funded_liquidity, '99999999999999999999999999999999999999.9999999999999999999') + coalesce(positions.sum_margin, 0)) as tvl, " +
		"t_pools.pool_address " +
		"FROM t_pools " +
		"left join " +
		"(select sum(to_number(margin, '99999999999999999999999999999999999999.9999999999999999999')) as sum_margin, pool_address " +
		"from t_positions " +
		"group by pool_address) as positions on t_pools.pool_address = positions.pool_address) tvl " +
		"left join (select t_pools.pool_address, t_pools.trade_pair, " +
		"coalesce(sum(case when position('Inverse' in t_pools.trade_pair) > 0 " +
		"then to_number(size, '99999999999999999999999999999999999999.9999999999999999999') * to_number(open_price, '99999999999999999999999999999999999999.9999999999999999999') / 1000000000000000000.000000000000000000000 " +
		"else to_number(size, '99999999999999999999999999999999999999.9999999999999999999') end), 0) as sum_size_24h " +
		"from t_positions " +
		"right join t_pools " +
		"on t_pools.pool_address = t_positions.pool_address " +
		"and ((close_block_height = 0 and " +
		"open_block_height >= (select max(block_height) from t_trade_token_price_change) - (24 * 3600 / 10)) " +
		"or close_block_height >= (select max(block_height) from t_trade_token_price_change) - (24 * 3600 / 10)) " +
		"group by t_pools.pool_address, t_pools.trade_pair " +
		"order by t_pools.trade_pair) v24 on tvl.pool_address = v24.pool_address " +
		"left join " +
		"(select tt.pool_address, tt.price " +
		"from (select pool_address, max(block_height) block_height " +
		"from t_trade_token_price_change " +
		"group by pool_address) tmp " +
		"left join t_trade_token_price_change tt " +
		"on tt.pool_address = tmp.pool_address " +
		"and tt.block_height = tmp.block_height) ttpc " +
		"on tvl.pool_address = ttpc.pool_address"

	if keyword != "" {
		totalSql = totalSql + " where pool_symbol = '" +
			keyword + "' or trade_symbol = '" +
			keyword + "' or pool_address = '" +
			keyword + "'"

		poolSql = poolSql + " where tvl.pool_symbol = '" +
			keyword + "' or trade_symbol = '" +
			keyword + "' or tvl.pool_address = '" +
			keyword + "' order by tvl desc limit " + strconv.Itoa(size) + " offset " + strconv.Itoa(page*size)
	} else {
		poolSql = poolSql + " order by tvl desc limit " + strconv.Itoa(size) + " offset " + strconv.Itoa(page*size)
	}

	num, err := db.Query(totalSql)

	var count int
	for num.Next() {
		if err = num.Scan(&count); err != nil {
			log.Fatal("PG Rows Scan Failed: ", err)
		}
		fmt.Println(count)
	}

	rows, err := db.Query(poolSql)

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
		var price string
		var tvl string
		var volume24h string
		var poolSymbol string
		var poolAddress string
		var poolDecimal string

		if err := rows.Scan(&poolAddress, &tradePair, &poolSymbol, &fee, &price, &tvl, &volume24h, &poolDecimal); err != nil {
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
			PoolDecimal: poolDecimal,
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
