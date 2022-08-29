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
	}

	SourceDRs struct {
		Items []SourceDR
	}

	SourceDRFilename struct {
		Id       int
		Filedate string
		Filename string
	}
)

func (s *SourceDRs) AddSrcDR(item SourceDR) []SourceDR {
	s.Items = append(s.Items, item)
	return s.Items
}
