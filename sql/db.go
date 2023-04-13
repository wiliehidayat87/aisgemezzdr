package sql

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"aisgemezzdr/items"
	Items "aisgemezzdr/items"

	Lib "github.com/wiliehidayat87/mylib"

	_ "github.com/go-sql-driver/mysql"
)

func BufferInsertOnString(dr Items.SourceDR) string {

	return fmt.Sprintf(`(DEFAULT, '%s', '%s', '%s', '%s', %d, %d, '%s', '%s', '%s', '%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d),`, dr.Filedate, dr.Filename, dr.FR_DN_leg, dr.MessageId, dr.Recipient, dr.Sender, dr.MMStatus, dr.StatusCode, dr.StatusText, dr.NtType, dr.Channel, dr.MSISDN, dr.LinkedId, dr.SSSActionReport, dr.GMessageId, dr.GMessageIdDate, dr.UserServiceNo, dr.MessageSequenceId, dr.Bearer, dr.BillInfo, dr.ClassOfService, dr.Vpkgid, dr.Timestamp, dr.CCT)
}

func MultiPutDR(db *sql.DB, table string, multiDR string) {

	SQL := `INSERT INTO ` + table + ` VALUES ` + multiDR

	res, err := db.Exec(SQL)

	if err != nil {

		fmt.Println(
			fmt.Sprintf("MultiPutDR - error [%#v] query : %s", err, SQL),
		)

		panic(err)

	} else {

		count, err := res.RowsAffected()

		fmt.Println(
			fmt.Sprintf("MultiPutDR - query: %s, return: %d, err: %#v", SQL, count, err),
		)

	}
}

func IsDRAlreadyProcessed(db *sql.DB, table string, filedate string, filename string) bool {

	SQL := fmt.Sprintf(`SELECT id FROM `+table+` WHERE filedate = '%s' AND filename = '%s'`, filedate, filename)

	rows, err := db.Query(SQL)
	if err != nil {
		// handle this error better than this
		fmt.Println(
			fmt.Sprintf("IsDRAlreadyProcessed - error [%#v] query : %s", err, SQL),
		)
	}
	defer rows.Close()

	var c Items.SourceDR

	for rows.Next() {

		err = rows.Scan(&c.Id)

		if err != nil {

			fmt.Println(
				fmt.Sprintf("IsDRAlreadyProcessed - Failed to process this filtering: %#v", err),
			)

		}

	}

	fmt.Println(
		fmt.Sprintf("IsDRAlreadyProcessed - query: %s, return: %#v", SQL, c),
	)

	if c.Id > 0 {
		return true
	} else {
		return false
	}
}

func PutDRFile(db *sql.DB, table string, dr Items.SourceDRFilename) {

	SQL := fmt.Sprintf(`INSERT INTO `+table+` VALUE (DEFAULT, '%s', '%s')`, dr.Filedate, dr.Filename)

	res, err := db.Exec(SQL)

	if err != nil {

		fmt.Println(
			fmt.Sprintf("PutDRFile - error [%#v] query : %s", err, SQL),
		)

		panic(err)

	} else {

		count, err := res.RowsAffected()

		fmt.Println(
			fmt.Sprintf("PutDRFile - query: %s, return: %d, err: %#v", SQL, count, err),
		)

	}
}

func SelectGroupSourceDR(db *sql.DB, tableSRCDR string, tableTrx string, filedate string, serviceid string, subject string, yesterday bool) ([]Items.SourceDR, int) {

	weekday := time.Now().Weekday()

	if weekday.String() == "Thursday" && yesterday {
		tableTrx = Lib.Concat(tableTrx, "_", Lib.GetYesterdayWithFormat(1, "20060102"))
	}

	SQL := fmt.Sprintf(`SELECT t.* FROM (SELECT fr_dn_leg, cct, gmessageid_date, sender, mmstatus, statuscode, statustext, channel, sssactionreport, HOUR(timestamp) AS time_attempt, COUNT(1) AS total FROM `+tableSRCDR+` WHERE filedate = '%s' AND gmessageid_date = '%s' AND msisdn IN (SELECT msisdn FROM `+tableTrx+` WHERE msgtimestamp BETWEEN '%s 00:00:00' AND '%s 23:59:59' AND service_id = '%s' AND SUBJECT = '%s' AND price <> 0) GROUP BY fr_dn_leg, cct, gmessageid_date, sender, mmstatus, statuscode, statustext, channel, sssactionreport, time_attempt) AS t ORDER BY t.fr_dn_leg DESC;`, filedate, filedate, filedate, filedate, serviceid, subject)

	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(
			fmt.Sprintf("SelectGroupSourceDR - error [%#v] query : %s", err, SQL),
		)
	}
	defer rows.Close()

	var row []Items.SourceDR

	var m Items.SourceDRs

	count := 0

	for rows.Next() {

		var t Items.SourceDR

		err = rows.Scan(&t.FR_DN_leg, &t.CCT, &t.GMessageIdDate, &t.Sender, &t.MMStatus, &t.StatusCode, &t.StatusText, &t.Channel, &t.SSSActionReport, &t.Timestamp, &t.Total)

		if err != nil {

			fmt.Println(
				fmt.Sprintf("SelectGroupSourceDR - Failed to process this filtering: %#v", err),
			)

		}

		row = m.AddSrcDR(t)

		count++
	}

	fmt.Println(
		fmt.Sprintf("SelectFromSourceDR - query: %s, return: %d, err: %#v", SQL, count, err),
	)

	return row, count
}

func SelectFromSourceDR(db *sql.DB, st Items.SourceDR, subject string, yesterday bool) ([]Items.SourceDR, int) {

	weekday := time.Now().Weekday()
	tableTrx := st.TblTrx

	if weekday.String() == "Thursday" && yesterday {
		tableTrx = Lib.Concat(tableTrx, "_", Lib.GetYesterdayWithFormat(1, "20060102"))
	}

	service_id := strconv.Itoa(st.Sender)

	SQL := fmt.Sprintf(`SELECT id, messageid, msisdn FROM %s WHERE filedate = '%s' AND gmessageid_date = '%s' AND fr_dn_leg = '%s' and cct = %d AND sender = %d AND statuscode = '%s' AND statustext = '%s' AND channel = '%s' AND mmstatus = '%s' AND sssactionreport = '%s' AND HOUR(timestamp) = '%s' AND msisdn IN (SELECT msisdn FROM %s WHERE msgtimestamp BETWEEN '%s 00:00:00' AND '%s 23:59:59' AND service_id = '%s' AND SUBJECT = '%s' AND price <> 0)`, st.TblSourceDR, st.Filedate, st.Filedate, st.FR_DN_leg, st.CCT, st.Sender, st.StatusCode, st.StatusText, st.Channel, st.MMStatus, st.SSSActionReport, st.Timestamp, tableTrx, st.Filedate, st.Filedate, service_id, subject)

	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(
			fmt.Sprintf("SelectFromSourceDR - error [%#v] query : %s", err, SQL),
		)
	}
	defer rows.Close()

	var row []Items.SourceDR

	var m Items.SourceDRs

	count := 0

	for rows.Next() {

		var t Items.SourceDR

		err = rows.Scan(&t.Id, &t.MessageId, &t.MSISDN)

		if err != nil {

			fmt.Println(
				fmt.Sprintf("SelectFromSourceDR - Failed to process this filtering: %#v", err),
			)

		}

		row = m.AddSrcDR(t)

		count++
	}

	fmt.Println(
		fmt.Sprintf("SelectFromSourceDR - query: %s, return: %d, err: %#v", SQL, count, err),
	)

	return row, count
}

func GetTrx(db *sql.DB, table string, trxdate string, serviceid string, msisdn int) ([]items.Trx, int) {

	SQL := fmt.Sprintf(`SELECT id, msgindex, msgstatus, fr_closereason, dn_closereason FROM `+table+` WHERE msgtimestamp BETWEEN '%s 00:00:00' AND '%s 23:59:59' AND service_id = '%s' AND subject = 'RENEWAL' AND msgindex = '' AND msgstatus = 'SENT' AND price <> 0 AND msisdn = %d`, trxdate, trxdate, serviceid, msisdn)

	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(
			fmt.Sprintf("GetTrx - error [%#v] query : %s", err, SQL),
		)
	}
	defer rows.Close()

	var row []Items.Trx

	var m Items.Trxs

	count := 0

	for rows.Next() {

		var t Items.Trx

		err = rows.Scan(&t.Id, &t.Msgindex, &t.MsgStatus, &t.FRClosereason, &t.DNClosereason)

		if err != nil {

			fmt.Println(
				fmt.Sprintf("GetTrx - Failed to process this filtering: %#v", err),
			)

		}

		row = m.AddTrx(t)

		count++
	}

	// fmt.Println(
	// 	fmt.Sprintf("GetTrx - query: %s, return: %d, err: %#v", SQL, count, err),
	// )

	return row, count
}

func UpdateTrx(db *sql.DB, t Items.SourceDR, s string, subject string, yesterday bool) {

	var (
		SQL          string
		msgtimestamp string
	)

	weekday := time.Now().Weekday()
	tableTrx := t.TblTrx

	if weekday.String() == "Thursday" && yesterday {
		tableTrx = Lib.Concat(tableTrx, "_", Lib.GetYesterdayWithFormat(1, "20060102"))
	}

	service_id := strconv.Itoa(t.Sender)

	if t.FR_DN_leg == "FR" {

		time_attempt, _ := strconv.Atoi(t.Timestamp)

		if time_attempt >= 0 && time_attempt < 15 {
			msgtimestamp = fmt.Sprintf("msgtimestamp BETWEEN '%s 00:00:00' AND '%s 14:59:59'", t.Filedate, t.Filedate)
		} else {
			msgtimestamp = fmt.Sprintf("msgtimestamp BETWEEN '%s 15:00:00' AND '%s 23:59:59'", t.Filedate, t.Filedate)
		}

		SQL = fmt.Sprintf(`UPDATE %s SET msg_type = 'FR', msgstatus = '%s', fr_closereason = '%s' WHERE `+msgtimestamp+` AND service_id = '%s' AND subject = '%s' AND msgstatus = 'SENT' AND price <> 0 AND msisdn IN (%s)`, tableTrx, t.MsgStatus, t.StatusText, service_id, subject, s)

	} else if t.FR_DN_leg == "DN" {

		time_attempt, _ := strconv.Atoi(t.Timestamp)

		if time_attempt >= 0 && time_attempt < 15 {
			msgtimestamp = fmt.Sprintf("msgtimestamp BETWEEN '%s 00:00:00' AND '%s 14:59:59'", t.Filedate, t.Filedate)
		} else {
			msgtimestamp = fmt.Sprintf("msgtimestamp BETWEEN '%s 15:00:00' AND '%s 23:59:59'", t.Filedate, t.Filedate)
		}

		dn := Lib.Concat(t.MMStatus, "|", t.StatusCode, "|", t.StatusText)

		SQL = fmt.Sprintf(`UPDATE %s SET msgstatus = '%s', dn_closereason = '%s' WHERE msg_type = 'FR' AND `+msgtimestamp+` AND service_id = '%s' AND subject = '%s' AND price <> 0 AND msisdn IN (%s)`, tableTrx, t.MsgStatus, dn, service_id, subject, s)
	}

	res, err := db.Exec(SQL)

	if err != nil {

		fmt.Println(
			fmt.Sprintf("UpdateTrx - error [%#v] query : %s", err, SQL),
		)

		panic(err)

	} else {

		count, err := res.RowsAffected()

		fmt.Println(
			fmt.Sprintf("UpdateTrx - query: %s, return: %d, err: %#v", SQL, count, err),
		)

	}
}

func GetDRPullSchedules(db *sql.DB) []items.DRPullSchedules {

	SQL := fmt.Sprintf(`SELECT id, subject, dr_pull_date, start_time, end_time, types, tbl FROM aisnew.dr_pull_schedules WHERE status = 0 AND exe_pull <= NOW()`)

	rows, err := db.Query(SQL)
	if err != nil {
		// handle this error better than this
		fmt.Println(
			fmt.Sprintf("GetDRPullSchedules - error [%#v] query : %s", err, SQL),
		)
	}
	defer rows.Close()

	var drs []items.DRPullSchedules

	for rows.Next() {

		var dr items.DRPullSchedules

		err = rows.Scan(&dr.Id, &dr.Subject, &dr.DRPullDate, &dr.StartTime, &dr.EndTime, &dr.Types, &dr.Tbl)

		if err != nil {

			fmt.Println(
				fmt.Sprintf("GetDRPullSchedules - Failed to process this filtering: %#v", err),
			)

		}

		drs = append(drs, dr)
	}

	fmt.Println(
		fmt.Sprintf("GetDRPullSchedules - query: %s, return: %#v", SQL, drs),
	)

	return drs
}

func UpdateStatusSchedules(db *sql.DB, dr items.DRPullSchedules) {

	SQL := fmt.Sprintf(`UPDATE aisnew.dr_pull_schedules SET status = %d WHERE id = %d`, dr.Status, dr.Id)

	res, err := db.Exec(SQL)

	if err != nil {

		fmt.Println(
			fmt.Sprintf("UpdateStatusSchedules - error [%#v] query : %s", err, SQL),
		)

		panic(err)

	} else {

		count, err := res.RowsAffected()

		fmt.Println(
			fmt.Sprintf("UpdateStatusSchedules - query: %s, return: %d, err: %#v", SQL, count, err),
		)

	}
}

func GetFromSourceDR(db *sql.DB, st Items.SourceDR) ([]Items.SourceDR, int) {

	SQL := fmt.Sprintf(`SELECT msisdn, fr_dn_leg, mmstatus, statuscode, statustext FROM aisnew.source_dr WHERE filedate = '%s' AND timestamp BETWEEN '%s' AND '%s';`, st.Filedate, st.StartTime, st.EndTime)

	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(
			fmt.Sprintf("GetFromSourceDR - error [%#v] query : %s", err, SQL),
		)
	}
	defer rows.Close()

	var row []Items.SourceDR

	count := 0

	for rows.Next() {

		var t Items.SourceDR

		err = rows.Scan(&t.MSISDN, &t.FR_DN_leg, &t.MMStatus, &t.StatusCode, &t.StatusText)

		if err != nil {

			fmt.Println(
				fmt.Sprintf("SelectFromSourceDR - Failed to process this filtering: %#v", err),
			)

		}

		row = append(row, t)

		count++
	}

	fmt.Println(
		fmt.Sprintf("SelectFromSourceDR - query: %s, return: %d, err: %#v", SQL, count, err),
	)

	return row, count
}

func PullUpdate(db *sql.DB, dr items.DataDR) {

	var SQL string

	if dr.FRDNLeg == "FR" {

		SQL = fmt.Sprintf(`UPDATE %s SET msg_type = '%s', msgstatus = '%s', fr_closereason = '%s' WHERE subject = '%s' AND msgtimestamp BETWEEN CONCAT('%s', ' ', '%s') AND ('%s', ' ', '%s') AND (msgstatus != "DELIVERED" AND dn_closereason != "Retrieved|1000|external:DELIVRD:000") AND msisdn = '%d'`, dr.Tbl, "FR", "FAILED", dr.StatusText, dr.Subject, dr.TrxDate, dr.StartTime, dr.TrxDate, dr.EndTime, dr.Msisdn)

	} else {

		SQL = fmt.Sprintf(`UPDATE %s SET msg_type = '%s', msgstatus = '%s', dn_closereason = '%s|%s|%s' WHERE subject = '%s' AND msgtimestamp BETWEEN CONCAT('%s', ' ', '%s') AND ('%s', ' ', '%s') AND msisdn = '%d';`, dr.Tbl, "FR", "DELIVERED", dr.MMStatus, dr.StatusCode, dr.StatusText, dr.Subject, dr.TrxDate, dr.StartTime, dr.TrxDate, dr.EndTime, dr.Msisdn)
	}

	res, err := db.Exec(SQL)

	if err != nil {

		fmt.Println(
			fmt.Sprintf("PullUpdate - error [%#v] query : %s", err, SQL),
		)

		panic(err)

	} else {

		count, err := res.RowsAffected()

		fmt.Println(
			fmt.Sprintf("PullUpdate - query: %s, return: %d, err: %#v", SQL, count, err),
		)

	}
}
