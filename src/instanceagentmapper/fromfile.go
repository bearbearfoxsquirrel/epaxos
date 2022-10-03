package instanceagentmapper

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type PAM struct { // proposer acceptor mapping
	Proposers []int32 `json:"P"`
	Acceptors []int32 `json:"A"`
}

//type pams struct {
//	pams []PAM `json:""`
//}

func ReadFromFile(loc string) []PAM {
	jsonFile, err := os.Open(loc)
	defer jsonFile.Close()
	if err != nil {
		println(err)
	}
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var pam []PAM
	json.Unmarshal(byteValue, &pam)
	return pam
}

func GetAMap(pam []PAM) [][]int32 {
	amap := make([][]int32, 0, len(pam))
	for _, m := range pam {
		amap = append(amap, m.Acceptors)
	}
	return amap
}

func GetPMap(pam []PAM) [][]int32 {
	pmap := make([][]int32, 0, len(pam))
	for _, m := range pam {
		pmap = append(pmap, m.Proposers)
	}
	return pmap
}
