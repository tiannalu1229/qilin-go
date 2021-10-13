package web

import (
	"fmt"
	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
	qilin_request "github.com/tiannalu1229/qilin-go/qilin-request"
	"net/http"
)

func TradePairService() {
	r := gin.Default()
	r.POST("/getTradePair", func(c *gin.Context) {
		buf := make([]byte, 1024)
		n, _ := c.Request.Body.Read(buf)
		a := qilin_request.Condition{}
		jsoniter.Unmarshal(buf, &a)
		fmt.Println(n)
		keyword := a.Keyword
		page := a.Page
		size := a.Size

		result := qilin_request.SelectTradePair(keyword, page, size)

		c.JSON(http.StatusOK, gin.H{
			"result": result,
		})
	})
	r.Run(":3001")
}
