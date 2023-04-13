package items

type (
	SourceDR struct {
		Id                int
		Filedate          string
		Filename          string
		FR_DN_leg         string
		MessageId         string
		Recipient         int
		Sender            int
		MMStatus          string
		StatusCode        string
		StatusText        string
		NtType            string
		Channel           string
		MSISDN            int
		LinkedId          string
		SSSActionReport   string
		GMessageId        string
		GMessageIdDate    string
		UserServiceNo     string
		MessageSequenceId string
		Bearer            string
		BillInfo          string
		ClassOfService    string
		Vpkgid            string
		Timestamp         string
		CCT               int
		Total             int
		MsgStatus         string
		TblSourceDR       string
		TblTrx            string
		StartTime         string
		EndTime           string
	}

	SourceDRs struct {
		Items []SourceDR
	}

	SourceDRFilename struct {
		Id       int
		Filedate string
		Filename string
	}

	DRPullSchedules struct {
		Id         int
		Subject    string
		DRPullDate string
		StartTime  string
		EndTime    string
		ExePull    string
		Status     int
		Types      string
		Tbl        string
	}

	DataDR struct {
		Msisdn     int    `json:"msisdn"`
		FRDNLeg    string `json:"frdnleg"`
		MMStatus   string `json:"mmstatus"`
		StatusCode string `json:"statuscode"`
		StatusText string `json:"statustext"`
		Tbl        string `json:"tbl"`
		TrxDate    string `json:"trxdate"`
		Subject    string `json:"subject"`
		StartTime  string `json:"starttime"`
		EndTime    string `json:"endtime"`
	}
)

func (s *SourceDRs) AddSrcDR(item SourceDR) []SourceDR {
	s.Items = append(s.Items, item)
	return s.Items
}
