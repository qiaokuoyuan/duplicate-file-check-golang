package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// 定义配置文件
type Conf struct {
	CheckFolder         string
	MysqlUser           string
	MysqlPassword       string
	MysqlPort           string
	MysqlDBName         string
	SqlFileExistByMD5   string
	SqlInsertFile       string
	DiskSn              string
	DeleteDuplicateFile bool
}

// 定义读取配置文件函数
func getConf() *Conf {
	file, _ := os.Open("./conf.json")
	defer file.Close()
	decoder := json.NewDecoder(file)
	conf := Conf{}
	err := decoder.Decode(&conf)
	if err != nil {
		fmt.Println("Error:", err)
	}

	return &conf
}

// 确认输入是否有效
func confirmConf(c *Conf) bool {

	// 先验证序列号
	fmt.Printf("%q\r\n", "请再次输入磁盘序列号")

	inputText := ""

	fmt.Printf("磁盘序列号【%s】\r\n", c.DiskSn)
	fmt.Scanln(&inputText)
	if strings.Compare(inputText, c.DiskSn) != 0 {
		fmt.Printf("磁盘序列号不一致，您的输入【%q】，目标输入【%q】\r\n", inputText, c.DiskSn)
		return false
	}

	// 如果删除文件，则需要确认
	if c.DeleteDuplicateFile {
		fmt.Printf("%q\r\n", "您选择了【删除】重复文件，请输入【yes】确认")
		inputText = ""
		fmt.Scanln(&inputText)
		if inputText != "yes" {
			fmt.Printf("您的输入是【%q】，不是【yes】\r\n", inputText)
			return false
		}
	}

	return true
}

// 获取文件md5
func getFileMd5(fileDir string) string {

	// 打开文件
	file, _ := os.Open(fileDir)
	defer file.Close()

	// 生成 md5
	md5hash := md5.New()
	io.Copy(md5hash, file)

	return fmt.Sprintf("%x", md5hash.Sum(nil))
}

func main() {

	var (
		sql_exist_query int
		deal_count      int
	)

	// 读取配置文件
	conf := getConf()

	// 验证输入
	if !confirmConf(conf) {
		return
	}

	fmt.Println("程序开始")

	// 打开数据库
	db, _ := sql.Open("mysql", conf.MysqlUser+":"+conf.MysqlPassword+"@tcp(127.0.0.1:"+conf.MysqlPort+")/"+conf.MysqlDBName)
	defer db.Close()

	// 预定义保存文件
	saveFile := func(fileInfo os.FileInfo, fileDir string, fileMd5 string) {
		db.Exec(conf.SqlInsertFile, fileInfo.Name(), fileInfo.Size(), fileDir, conf.DiskSn, fileMd5)

	}

	deal_count = 0

	// 递归所有子文件
	filepath.Walk(conf.CheckFolder, func(fileDir string, fileInfo os.FileInfo, _ error) error {

		if !fileInfo.IsDir() {

			deal_count = deal_count + 1

			fmt.Println(deal_count)

			// 重置是否存在
			sql_exist_query = 0

			// 获取文件md5
			fileMd5 := getFileMd5(fileDir)

			// 检查文件是否存在

			// 获取查询结果
			err := db.QueryRow(conf.SqlFileExistByMD5, fileMd5).Scan(&sql_exist_query)

			// 如果查询不出来，则表示没有
			if err != nil {
				saveFile(fileInfo, fileDir, fileMd5)
			} else if conf.DeleteDuplicateFile {
				// 如果查询出来重复，且需要删除
				os.Remove(fileDir)
			} else {
				saveFile(fileInfo, fileDir, fileMd5)
			}

		}
		return nil
	})

}
