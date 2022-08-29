package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// PREDEFINE CONFIG OF THIS PROJECT
const (
	AccessLogTimeZone = "Asia/Jakarta"
)

type (
	SqlConf struct {
		Host                string
		Port                int
		Username            string
		Password            string
		Database            string
		TblSourceDR         string
		TblSourceDRFilename string
		TblTrx              string
	}
)

func (c *SqlConf) SetMySQL(appname string) {

	// Set the file name of the configurations file
	viper.SetConfigName("mysql")

	// Set the path to look for the configurations file
	viper.AddConfigPath(appname + "/config/")

	// Enable VIPER to read Environment Variables
	viper.AutomaticEnv()

	viper.SetConfigType("yml")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Error reading config file, %s", err)
	}

	c.Host = viper.GetString("MYSQL.HOST")
	c.Port = viper.GetInt("MYSQL.PORT")
	c.Username = viper.GetString("MYSQL.USERNAME")
	c.Password = viper.GetString("MYSQL.PASSWORD")
	c.Database = viper.GetString("MYSQL.DATABASE")

	c.TblSourceDR = viper.GetString("TABLE.SOURCEDR")
	c.TblSourceDRFilename = viper.GetString("TABLE.SOURCEDRFILENAME")
	c.TblTrx = viper.GetString("TABLE.TRX")

}

func (c *SqlConf) GetMySQL() string {

	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
		c.Username, c.Password, c.Host, c.Port, c.Database,
	)
}
