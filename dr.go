package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	Conf "aisgemezzdr/config"
	Items "aisgemezzdr/items"
	SQL "aisgemezzdr/sql"

	_ "github.com/go-sql-driver/mysql"
	Lib "github.com/wiliehidayat87/mylib"
	"github.com/wiliehidayat87/rmqp"
)

var (
	DB                  *sql.DB
	CFG                 Conf.SqlConf
	APP_PATH            string
	DR_PATH             string
	DESTINATION_DONE_DR string
	Rabbit              rmqp.AMQP
)

func init() {

	var err error

	APP_PATH = strings.TrimSuffix(Lib.ReadASingleValueInFile(".env", "APP_PATH"), "\n")
	DR_PATH = strings.TrimSuffix(Lib.ReadASingleValueInFile(".env", "DR_PATH"), "\n")
	DESTINATION_DONE_DR = strings.TrimSuffix(Lib.ReadASingleValueInFile(".env", "DESTINATION_DONE_DR"), "\n")

	// Setup Database MYSQL
	CFG.SetMySQL(APP_PATH)

	DB, err = sql.Open("mysql", CFG.GetMySQL())

	// this init should be check
	// and throw an error if the database not connected

	if err != nil {

		// panic the function then hard exit
		fmt.Println(fmt.Sprintf("[x] An Error occured when establishing of the database : %#v", err))

		panic(err)

	} else {

		fmt.Println("[v] Database successful established")
	}

}
func main() {

	trigger := os.Args[1]

	if trigger == "put" {

		filedate := os.Args[2]

		if filedate == "CURDATE" {
			filedate = Lib.GetDate("2006-01-02")
		}

		put(filedate)

	} else if trigger == "reckon" {

		trxdate := os.Args[2]

		if trxdate == "CURDATE" {
			trxdate = Lib.GetDate("2006-01-02")
		}

		serviceid := os.Args[3]
		subject := os.Args[4]
		yesterday, _ := strconv.ParseBool(os.Args[5])

		reckon(trxdate, serviceid, subject, yesterday)

	} else if trigger == "push_update" {

		pushUpdate()

	} else if trigger == "pull_update" {

		pullUpdate()

	} else if trigger == "move" {

		trxdate := os.Args[2]

		move(trxdate)
	}
}

func pushUpdate() {

	// Setup Rabbit MQ Connection URL
	Rabbit.SetAmqpURL(
		"localhost",
		5672,
		"adminxmp",
		"xmp2022",
	)

	// Setup Rabbit MQ Connection
	Rabbit.SetUpConnectionAmqp()

	// Setup Rabbit MQ Connection Channel
	Rabbit.SetUpChannel("direct", false, "E_DR", true, "Q_DR")

	timeDuration := time.Duration(1)

	for {

		// Setup / Init the log
		drs := SQL.GetDRPullSchedules(DB)

		if len(drs) > 0 {

			for _, dr := range drs {

				// Update setting status on process
				SQL.UpdateStatusSchedules(DB, Items.DRPullSchedules{Id: dr.Id, Status: 1})

				pushUpdateData(dr)

				// Update setting status done
				SQL.UpdateStatusSchedules(DB, Items.DRPullSchedules{Id: dr.Id, Status: 2})
			}

		}

		// Request per 1 minute
		time.Sleep(timeDuration * time.Minute)
	}
}

func pushUpdateData(dr Items.DRPullSchedules) {

	var m sync.Mutex

	m.Lock()

	srcGroup, totR := SQL.GetFromSourceDR(DB, Items.SourceDR{Filedate: dr.DRPullDate, StartTime: dr.StartTime, EndTime: dr.EndTime})

	if totR > 0 {

		for _, sdr := range srcGroup {

			corId := Lib.Concat("DRS", Lib.GetUniqId())

			reqBody, _ := json.Marshal(Items.DataDR{
				Msisdn:     sdr.MSISDN,
				FRDNLeg:    sdr.FR_DN_leg,
				MMStatus:   sdr.MMStatus,
				StatusCode: sdr.StatusCode,
				StatusText: sdr.StatusText,
				Tbl:        dr.Tbl,
				TrxDate:    dr.DRPullDate,
				StartTime:  dr.StartTime,
				EndTime:    dr.EndTime,
			})

			request := string(reqBody)

			eName := "E_DR"
			qName := "Q_DR"

			isPublished := Rabbit.IntegratePublish(
				qName,
				eName,
				"application/json",
				corId,
				request,
			)

			if isPublished {

				fmt.Println(fmt.Sprintf("[v] Published into %s: %s, Data: %s ...", qName, corId, request))

			} else {

				fmt.Println(fmt.Sprintf("[v] Failed published %s: %s, Data: %s ...", qName, corId, request))

			}

		}

	} else {

		fmt.Println(
			fmt.Sprintf("No DR to push"),
		)
	}

	defer m.Unlock()
}

func pullUpdate() {

	timeDuration := time.Duration(1)

	qName := "Q_DR"
	eName := "E_DR"

	// Setup Rabbit Queue Data
	messagesData := Rabbit.Subscribe(1, false, qName, eName, qName)

	// Loop forever listening incoming data
	forever := make(chan bool)

	// Set into goroutine this listener
	go func() {

		// Loop every incoming data
		for d := range messagesData {

			var dr Items.DataDR

			json.Unmarshal(d.Body, &dr)

			// Update DR
			SQL.PullUpdate(DB, dr)

			// Manual consume queue
			d.Ack(false)

			// Listener waiting ticker
			time.Sleep(timeDuration * time.Millisecond)
		}

	}()

	fmt.Println("[*] Waiting for data...")

	<-forever

}

func reckon(trxdate string, serviceid string, subject string, yesterday bool) {

	// Case reckoning should index all correlative selected column
	// Get Group by per all status DR
	srcGroup, totR := SQL.SelectGroupSourceDR(DB, CFG.TblSourceDR, CFG.TblTrx, trxdate, serviceid, subject, yesterday)

	if totR > 0 {

		x := 0
		for _, sg := range srcGroup {

			var st Items.SourceDR
			st.FR_DN_leg = sg.FR_DN_leg
			st.CCT = 11
			st.GMessageIdDate = sg.GMessageIdDate
			st.Sender = sg.Sender
			st.MMStatus = sg.MMStatus
			st.StatusCode = sg.StatusCode
			st.StatusText = sg.StatusText
			st.Channel = sg.Channel
			st.SSSActionReport = sg.SSSActionReport
			st.Filedate = trxdate
			st.Timestamp = sg.Timestamp
			st.TblSourceDR = CFG.TblSourceDR
			st.TblTrx = CFG.TblTrx

			if sg.StatusText == "external:success" || sg.StatusText == "external:DELIVRD:000" {
				st.MsgStatus = "DELIVERED"
			} else {
				st.MsgStatus = "FAILED"
			}

			// Select per status DR
			src, totalRecord := SQL.SelectFromSourceDR(DB, st, subject, yesterday)

			if totalRecord > 0 {

				var (
					i         int
					buffer    int
					sqlString string
				)

				i = 0
				sqlString = ""

				if st.FR_DN_leg == "DN" {
					buffer = 50
				} else {
					buffer = 500
				}

				for _, s := range src {

					sqlString += Lib.Concat(strconv.Itoa(s.MSISDN), ",")

					if i == buffer {

						sqlString = strings.TrimRight(sqlString, ",")

						SQL.UpdateTrx(DB, st, sqlString, subject, yesterday)
						sqlString = ""
						i = 0
					}

					i++
				}

				if i > 0 {

					sqlString = strings.TrimRight(sqlString, ",")

					SQL.UpdateTrx(DB, st, sqlString, subject, yesterday)
					sqlString = ""
					i = 0

				}
			}

			x++
		}
	}
}

func put(filedate string) {

	files, err := ioutil.ReadDir(DR_PATH)
	if err != nil {
		log.Fatal(err)
	}

	_f := 0
	for _, f := range files {

		var m sync.Mutex

		m.Lock()

		filebase := strings.Replace(filedate, "-", "", -1)

		if strings.Contains(f.Name(), filebase) {

			if !SQL.IsDRAlreadyProcessed(DB, CFG.TblSourceDRFilename, filedate, f.Name()) {

				//fmt.Println(f.Name())
				data, _ := Lib.ReadGzFile(DR_PATH + "/" + f.Name())

				//split line
				ldata := strings.Split(string(data), "\n")

				i := 0
				buffer := 50
				sqlString := ""

				for _, line := range ldata {

					if line != "" {

						var m1 sync.Mutex

						m1.Lock()

						fmt.Println(fmt.Sprintf("source %s, dr : %s, total line : %d", f.Name(), line, _f))

						// split string
						cdata := strings.Split(line, "|")

						var sourceDR Items.SourceDR
						sourceDR.Filedate = filedate
						sourceDR.Filename = f.Name()
						sourceDR.FR_DN_leg = cdata[0]
						sourceDR.MessageId = cdata[1]
						sourceDR.Recipient, _ = strconv.Atoi(cdata[2])
						sourceDR.Sender, _ = strconv.Atoi(cdata[3])
						sourceDR.MMStatus = cdata[4]
						sourceDR.StatusCode = cdata[5]
						sourceDR.StatusText = cdata[6]
						sourceDR.NtType = cdata[7]
						sourceDR.Channel = cdata[8]
						sourceDR.MSISDN, _ = strconv.Atoi(cdata[9])
						sourceDR.LinkedId = cdata[10]
						sourceDR.SSSActionReport = cdata[11]
						sourceDR.GMessageId = cdata[12]

						if sourceDR.GMessageId != "" {
							gMsgId := strings.Split(sourceDR.GMessageId, "_")

							runes := []rune(gMsgId[1])
							sourceDR.GMessageIdDate = Lib.Concat(string(runes[0:4]), "-", string(runes[4:6]), "-", string(runes[6:8]))
						} else {

							FileName := strings.Split(sourceDR.Filename, "_")

							runes := []rune(FileName[1])
							sourceDR.GMessageIdDate = Lib.Concat(string(runes[0:4]), "-", string(runes[4:6]), "-", string(runes[6:8]))
						}

						sourceDR.UserServiceNo = cdata[13]
						sourceDR.MessageSequenceId = cdata[14]
						sourceDR.Bearer = cdata[15]
						sourceDR.BillInfo = cdata[16]
						sourceDR.ClassOfService = cdata[17]
						sourceDR.Vpkgid = cdata[18]
						sourceDR.Timestamp = cdata[19]
						sourceDR.CCT, _ = strconv.Atoi(cdata[20])

						//SQL.PutDR(DB, CFG.TblSourceDR, sourceDR)

						sqlString += SQL.BufferInsertOnString(sourceDR)

						if i == buffer {

							sqlString = strings.TrimRight(sqlString, ",")

							SQL.MultiPutDR(DB, CFG.TblSourceDR, sqlString)
							sqlString = ""
							i = 0
						}

						_f++
						i++

						m1.Unlock()
					}
				}

				if i > 0 {

					sqlString = strings.TrimRight(sqlString, ",")

					if sqlString != "" {

						SQL.MultiPutDR(DB, CFG.TblSourceDR, sqlString)
						sqlString = ""
						i = 0
					}

				}

				var drFile Items.SourceDRFilename
				drFile.Filedate = filedate
				drFile.Filename = f.Name()

				SQL.PutDRFile(DB, CFG.TblSourceDRFilename, drFile)

			} else {

				fmt.Println("DR " + f.Name() + " already processed!")
			}

			if Lib.Copy(DR_PATH+"/"+f.Name(), DESTINATION_DONE_DR+"/"+f.Name()) {

				// 	// Remove
				os.Remove(DR_PATH + "/" + f.Name())
			}

		}

		m.Unlock()
	}
}

func move(filedate string) {

	files, err := ioutil.ReadDir(DR_PATH)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {

		var m sync.Mutex

		m.Lock()
		if Lib.Copy(DR_PATH+"/"+f.Name(), DESTINATION_DONE_DR+"/"+f.Name()) {

			if _, err := os.Stat(DESTINATION_DONE_DR + "/" + f.Name()); err == nil {

				fmt.Printf("File exists then remove\n")

				// 	// Remove
				os.Remove(DR_PATH + "/" + f.Name())

			} else {
				fmt.Printf("File does not exist\n")
			}
		} else {
			fmt.Printf("File failed to copy : %s\n", DR_PATH+"/"+f.Name())
		}

		m.Unlock()
	}
}
