package items

import (
	"database/sql"
	"encoding/json"
)

type Trx struct {
	Id            string         `json:"id"`
	InReplyTo     string         `json:"inreplyto"`
	Msgindex      string         `json:"msgindex"`
	Msgtimestamp  string         `json:"msgtimestamp"`
	Adn           string         `json:"adn"`
	Msisdn        string         `json:"msisdn"`
	Operator      string         `json:"operator"`
	Msgdata       string         `json:"msgdata"`
	MsgType       string         `json:"msgtype"`
	MsgStatus     string         `json:"msgstatus"`
	FRClosereason string         `json:"frclosereason"`
	DNClosereason string         `json:"dnclosereason"`
	Serviceid     string         `json:"serviceid"`
	Servicename   string         `json:"servicename"`
	Keyword       string         `json:"keyword"`
	Serviceno     string         `json:"serviceno"`
	CCT           string         `json:"cct"`
	CPAction      string         `json:"cpaction"`
	Channel       string         `json:"channel"`
	Subject       string         `json:"subject"`
	Price         string         `json:"price"`
	Flag          sql.NullString `json:"flag"`
}

type Trxs struct {
	Items []Trx
}

func (s *Trxs) AddTrx(item Trx) []Trx {
	s.Items = append(s.Items, item)
	return s.Items
}

func (s *Trx) BuildJsonStringTrx(t Trx) string {

	byteArray, err := json.Marshal(t)

	if err != nil {
		panic(err)
	}

	return string(byteArray)
}
