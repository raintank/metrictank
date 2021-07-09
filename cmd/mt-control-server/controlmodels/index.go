package controlmodels

type IndexDelByQueryReq struct {
	Expr      []string `json:"expr" form:"expr" binding:"Required"`
	OlderThan int64    `json:"olderThan" form:"olderThan"`
	Method    string   `json:"method" form:"method" binding:";In(dry-run,delete,archive)"`
	Limit     int      `json:"limit" form:"limit"`
}

type IndexDelByQueryResp struct {
	Count      int         `json:"count"`
	Partitions map[int]int `json:"partitions"`
}

type IndexRestoreReq struct {
	Expr             []string `json:"expr" binding:"Required"`
	From             int64    `json:"from" binding:"Default(0)"`
	To               int64    `json:"to" binding:"Default(0)"`
	LastUpdateOffset int64    `json:"lastUpdateOffset" binding:"Default(0)"`
	PartitionStart   int      `json:"partitionStart" binding:"Default(0)"`
	NumPartitions    int      `json:"numPartitions" binding:"Default(-1)"`
}

type IndexRestoreResp struct {
	LogId string
}
