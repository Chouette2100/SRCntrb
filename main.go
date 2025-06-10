// Copyright © 2025 chouette2100@gmail.com
// Released under the MIT license
// https://opensource.org/licenses/mit-license.php
package main

import (
	// "fmt"
	// "sort"
	"strconv"
	// "strings"
	"time"

	"log"

	//	"bufio"
	//	"io"
	//	"io/ioutil"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	//	"database/sql"
	//	_ "github.com/go-sql-driver/mysql"

	// "SRGPC/ShowroomDBlib"
	"github.com/go-gorp/gorp"
	//	"Showroomlib"

	// "github.com/PuerkitoBio/goquery"

	// "net/http"
	//	"net/url"

	// "github.com/360EntSecGroup-Skylar/excelize"
	// lsdp "github.com/deltam/go-lsd-parametrized"

	"github.com/Chouette2100/exsrapi/v2"
	"github.com/Chouette2100/srapi/v2"
	"github.com/Chouette2100/srdblib/v2"
)

/*
00AA00 新規作成
00AB00 貢献ランキングの書き込みをユーザー単位のトランザクションとする
*/

const version = "00AB00"

/*
type EventRank struct {
	Order       int
	Rank        int
	Listner     string
	Lastname    string
	LsnID		int
	T_LsnID		int
	Point       int
	Incremental int
	Status      int
}

// 構造体のスライス
type EventRanking []EventRank

//	sort.Sort()のための関数三つ
func (e EventRanking) Len() int {
	return len(e)
}

func (e EventRanking) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

//	降順に並べる
func (e EventRanking) Less(i, j int) bool {
	//	return e[i].point < e[j].point
	return e[i].Point > e[j].Point
}

*/

/*
GetPointsContByApi()
イベントページのURLと配信者さんのIDから、イベント貢献ランキングのリストを取得します。

引数
EvnetName	string	イベント名、下記イベントページURLの"event_id"の部分

	https://www.showroom-live.com/event/event_id

ID_Account	string	配信者さんのID

	SHOWROOMへの登録順を示すと思われる6桁(以下)の数字です。アカウントとは違います。

戻り値
TotaScore	int
eventranking	struct

	Rank	int		リスナーの順位
	Point	int		リスナーの貢献ポイント
	Listner string	リスナーの名前

status		int
*/

/*
func GetPointsContByApi(
	client *http.Client,
	ieventid int,
	roomid int,
) (
	eventranking ShowroomDBlib.EventRanking,
	uidmap map[int]int,
	err error,
) {

	pranking, err := srapi.ApiEventContribution_ranking(client, ieventid, roomid)
	if err != nil {
		err = fmt.Errorf("ApiEventContribution_ranking() failed. %v", err)
		return
	}

	if len(pranking.Ranking) == 0 {
		err = fmt.Errorf("ApiEventContribution_ranking() returned empty ranking")
		return
	}

	uidmap = make(map[int]int)
	for i, r := range pranking.Ranking {
		er := ShowroomDBlib.EventRank{
			Order:   i + 1,
			Rank:    r.Rank,
			Listner: r.Name,
			Point:   r.Point,
			LsnID:   r.UserID,
		}
		eventranking = append(eventranking, er)

		uidmap[r.UserID] = i
	}

	return
}


/*
GetPointsCont()
イベントページのURLと配信者さんのIDから、イベント貢献ランキングのリストを取得します。

引数
EvnetName	string	イベント名、下記イベントページURLの"event_id"の部分

	https://www.showroom-live.com/event/event_id

ID_Account	string	配信者さんのID

	SHOWROOMへの登録順を示すと思われる6桁(以下)の数字です。アカウントとは違います。

戻り値
TotaScore	int
eventranking	struct

	Rank	int		リスナーの順位
	Point	int		リスナーの貢献ポイント
	Listner string	リスナーの名前

status		int

***
リスナーさんの日々のあるいは配信ごとの貢献ポイントの推移がすぐにわかれば配信者さんもいろいろ手の打ちよう(?)が
ありそうですが、「リスナーの名前」というのはリスナーさんが自由に設定・変更できるので貢献ポイントを追いかけて
行くのはけっこうたいへんです。
このプログラムではLevenshtein距離による類似度のチェックや貢献ランキングの特性を使ってニックネームの変更を追尾しています。
リスナーさんのuseridがわかればいいのですが、いろいろと面倒なところがあります。

「レーベンシュタイン距離 - Wikipedia」
https://ja.wikipedia.org/wiki/%E3%83%AC%E3%83%BC%E3%83%99%E3%83%B3%E3%82%B7%E3%83%A5%E3%82%BF%E3%82%A4%E3%83%B3%E8%B7%9D%E9%9B%A2

「カスタマイズしやすい重み付きレーベンシュタイン距離ライブラリをGoで書きました - サルノオボエガキ」
https://deltam.blogspot.com/2018/10/go.html

貢献ポイントをこまめに記録しておくと、減算ポイントが発生したときの原因アカウントの特定に使えないこともないです。
（実際やってみるとわかるのですが、これはこれでなかなかたいへんです）
なお、原因アカウントの特定、というのは犯人探しというような意味で言ってるわけじゃありませんので念のため。
*/
/*
func GetPointsCont(EventName, ID_Account string) (
	TotalScore int,
	eventranking ShowroomDBlib.EventRanking,
	status int,
) {

	status = 0

	//	貢献ランキングのページを開き、データ取得の準備をします。
	//	_url := "https://www.showroom-live.com/event/contribution/" + EventName + "?room_id=" + ID_Account
	ename := EventName
	ename_a := strings.Split(EventName, "?")
	if len(ename_a) == 2 {
		ename = ename_a[0]
	}
	_url := "https://www.showroom-live.com/event/contribution/" + ename + "?room_id=" + ID_Account

	resp, error := http.Get(_url)
	if error != nil {
		log.Printf("GetEventInfAndRoomList() http.Get() err=%s\n", error.Error())
		status = 1
		return
	}
	defer resp.Body.Close()

	var doc *goquery.Document
	doc, error = goquery.NewDocumentFromReader(resp.Body)
	if error != nil {
		log.Printf("GetEventInfAndRoomList() goquery.NewDocumentFromReader() err=<%s>.\n", error.Error())
		status = 1
		return
	}

	//	u := url.URL{}
	//	u.Scheme = doc.Url.Scheme
	//	u.Host = doc.Url.Host

	//	各リスナーの情報を取得します。
	//	var selector_ranking, selector_listner, selector_point, ranking, listner, point string
	var ranking, listner, point string
	var iranking, ipoint int
	var eventrank ShowroomDBlib.EventRank

	TotalScore = 0

	//	eventranking = make([]EventRank)

	doc.Find(".table-type-01:nth-child(2) > tbody > tr").Each(func(i int, s *goquery.Selection) {
		if i != 0 {

			//	データを一つ取得するたびに(戻り値となる)リスナー数をカウントアップします。
			//	NoListner++

			//	以下セレクターはブラウザの開発ツールを使って確認したものです。

			//	順位を取得し、文字列から数値に変換します。
			//	selector_ranking = fmt.Sprintf("table.table-type-01:nth-child(2) > tbody:nth-child(2) > tr:nth-child(%d) > td:nth-child(%d)", NoListner+2, 1)
			ranking = s.Find("td:nth-child(1)").Text()

			/.
				//	データがなくなったらbreakします。このときのNoListnerは通常100、場合によってはそれ以下です。
				if ranking == "" {
					break
				}
			./

			iranking, _ = strconv.Atoi(ranking)

			//	リスナー名を取得します。
			//	selector_listner = fmt.Sprintf("table.table-type-01:nth-child(2) > tbody:nth-child(2) > tr:nth-child(%d) > td:nth-child(%d)", NoListner+2, 2)
			listner = s.Find("td:nth-child(2)").Text()

			//	貢献ポイントを取得し、文字列から"pt"の部分を除いた上で数値に変換します。
			//	selector_point = fmt.Sprintf("table.table-type-01:nth-child(2) > tbody:nth-child(2) > tr:nth-child(%d) > td:nth-child(%d)", NoListner+2, 3)
			point = s.Find("td:nth-child(3)").Text()
			point = strings.Replace(point, "pt", "", -1)
			ipoint, _ = strconv.Atoi(point)
			TotalScore += ipoint

			//	戻り値となるスライスに取得したデータを追加します。
			eventrank.Rank = iranking
			eventrank.Point = ipoint
			eventrank.Listner = listner
			eventrank.Order = i
			eventranking = append(eventranking, eventrank)
		}
	})

	return
}

func MakeListInSheet(
	oldfilename,
	newfilename string,
	eventranking ShowroomDBlib.EventRanking,
	ncolw int,
	totalscore,
	totalincremental int,
) (
	status int,
) {

	status = 0

	no := len(eventranking)

	// Excelファイルをオープンする。
	//	fxlsx, err := excelize.OpenFile(EventID + ".xlsx")
	//	filename := event_id + "_" + room_id + "_" + fmt.Sprintf("%05d", serial) + ".xlsx"
	//	filename = "_tmp.xlsx"
	log.Printf(" inputfilename=<%s>\n", oldfilename)
	log.Printf(" outputfilename=<%s>\n", newfilename)
	fxlsx, err := excelize.OpenFile(oldfilename)
	if err != nil {
		log.Printf("<%v>\n", err)
		status = -1
		return
	}

	sheet1 := "Sheet1"
	sheet2 := "Sheet2"

	scolnew := CtoA(ncolw)
	//	scollast := CtoA(ncolw - 1)

	t19000101 := time.Date(1899, 12, 30, 0, 0, 0, 0, time.Local)
	tnow := time.Now()

	fxlsx.SetCellValue(sheet1, scolnew+"1", totalscore)
	fxlsx.SetCellValue(sheet2, scolnew+"1", totalincremental)

	//	fxlsx.SetCellValue(sheet, scolnew+"2", tnow)

	tserial := tnow.Sub(t19000101).Minutes() / 60.0 / 24.0
	fxlsx.SetCellValue(sheet1, scolnew+"3", tserial)
	fxlsx.SetCellValue(sheet2, scolnew+"3", tserial)

	fxlsx.SetCellValue(sheet1, scolnew+"4", tnow.Format("01/02 15:04"))
	fxlsx.SetCellValue(sheet2, scolnew+"4", tnow.Format("01/02 15:04"))

	for i := 0; i < no; i++ {
		loci := eventranking[i].Order
		srow := fmt.Sprintf("%d", loci+5)

		fxlsx.SetCellValue(sheet1, "A"+srow, eventranking[i].Rank)
		fxlsx.SetCellValue(sheet2, "A"+srow, eventranking[i].Rank)

		fxlsx.SetCellValue(sheet1, "C"+srow, eventranking[i].Listner)
		fxlsx.SetCellValue(sheet2, "C"+srow, eventranking[i].Listner)

		fxlsx.SetCellValue(sheet1, scolnew+srow, eventranking[i].Point)
		if eventranking[i].Incremental != -1 {
			fxlsx.SetCellValue(sheet2, scolnew+srow, eventranking[i].Incremental)
		} else {
			fxlsx.SetCellValue(sheet2, scolnew+srow, "n/a")
		}

		if eventranking[i].Lastname != "" {
			fxlsx.SetCellValue(sheet1, "B"+srow, eventranking[i].Lastname)
			/.
				//	Excelファイルの肥大化はこれが原因かも。あくまで"かも"。
				fxlsx.AddComment(sheet1, scollast+srow, `{"author":"Chouette: ","text":"`+eventranking[i].Lastname+`"}`)
			./
		} else {
			fxlsx.SetCellValue(sheet1, "B"+srow, nil)
		}
	}

	//	serial++
	//	filename = event_id + "_" + room_id + "_" + fmt.Sprintf("%05d", serial) + ".xlsx"
	//	Printf(" filename(out) = <%s>\n", filename)
	err = fxlsx.SaveAs(newfilename)

	if err != nil {
		log.Printf(" error in SaveAs() <%s>\n", err)
		status = -1
	}

	return
}

func CtoA(col int) (acol string) {
	acol = string(rune('A') + int32((col-1)%26))
	if int((col-1)/26) > 0 {
		acol = string(rune('A')+(int32((col-1)/26))-1) + acol
	}
	return
}
func CRtoA1(col, row int) (a1 string) {
	a1 = CtoA(col) + fmt.Sprintf("%d", row)
	return
}

func CopyFile(inputfile, outputfile string) (status int) {

	status = 0

	// read the whole file at once
	b, err := os.ReadFile(inputfile)
	if err != nil {
		//	panic(err)
		log.Printf("error <%v>\n", err)
		status = -1
		return
	}

	// write the whole body at once
	err = os.WriteFile(outputfile, b, 0644)
	if err != nil {
		//	panic(err)
		log.Printf("error <%v>\n", err)
		status = -2
	}
	return

}

func ReadListInSheet(
	oldfilename string,
) (
	eventranking ShowroomDBlib.EventRanking,
	ncolw int,
	status int,
) {

	status = 0

	// Excelファイルをオープンする。
	//	fxlsx, err := excelize.OpenFile(EventID + ".xlsx")
	//	filename := event_id + "_" + room_id + "_" + fmt.Sprintf("%05d", serial) + ".xlsx"
	log.Printf(" inputfilename=<%s>\n", oldfilename)
	//	filename = "_tmp.xlsx"
	fxlsx, err := excelize.OpenFile(oldfilename)
	if err != nil {
		log.Printf("<%v>\n", err)
		status = -1
		return
	}

	sheet := "Sheet1"

	for i := 4; ; i++ {
		//	value, _ := fxlsx.GetCellValue(sheet, CRtoA1(i, 4))
		value := fxlsx.GetCellValue(sheet, CRtoA1(i, 4))
		if value == "" {
			ncolw = i
			if ncolw == 4 {
				return
			}
			break
		}
	}

	var eventrank ShowroomDBlib.EventRank
	//	eventranking = make([]EventRank)

	scol := CtoA(ncolw - 1)
	for i := 0; i < 200; i++ {
		srow := fmt.Sprintf("%d", i+5)
		//	listner, _ := fxlsx.GetCellValue(sheet, "C"+srow)
		//	spoint, _ := fxlsx.GetCellValue(sheet, scol+srow)
		listner := fxlsx.GetCellValue(sheet, "C"+srow)
		spoint := fxlsx.GetCellValue(sheet, scol+srow)
		if listner == "" && spoint == "" {
			log.Println("*** break *** i=", i)
			break
		}

		eventrank.Order = i

		eventrank.Listner = listner

		eventrank.Point, _ = strconv.Atoi(spoint)

		eventranking = append(eventranking, eventrank)
	}

	sort.Sort(eventranking)

	return
}

func CompareEventRankingByApi(
	last_eventranking ShowroomDBlib.EventRanking,
	new_eventranking ShowroomDBlib.EventRanking,
	uidmap map[int]int,
) (
	final_eventranking ShowroomDBlib.EventRanking,
	totalincremental int,
) {

	//	for j := 0; j < len(last_eventranking); j++ {
	for j, ler := range last_eventranking {
		if idx, ok := uidmap[ler.LsnID]; ok {
			if ler.Point != -1 {
				incremental := new_eventranking[idx].Point - ler.Point
				totalincremental += incremental
				last_eventranking[j].Incremental = incremental
			} else {
				last_eventranking[j].Incremental = -1
			}
			last_eventranking[j].Rank = new_eventranking[idx].Rank
			last_eventranking[j].Point = new_eventranking[idx].Point
			last_eventranking[j].Order = new_eventranking[idx].Order
			if new_eventranking[idx].Listner == ler.Listner {
				last_eventranking[j].Lastname = ""
			} else {
				last_eventranking[j].Listner = new_eventranking[idx].Listner
				last_eventranking[j].Lastname = ler.Listner
			}
			new_eventranking[idx].Status = 1
		} else {
			//	同一のuseridのデータがみつからなかった。
			last_eventranking[j].Point = -1
			last_eventranking[j].Incremental = -1
			last_eventranking[j].Status = -1
			last_eventranking[j].Order = 999
			last_eventranking[j].Lastname = ""
			log.Printf("*****         【%s】  not found.\n", last_eventranking[j].Listner)

		}
	}
	//	既存のランキングになかった新規のリスナーを既存のランキングに追加する。
	//	ソートはしない。ソートするとExcelにあるデータと整合性がとれなくなる。
	//	つまり、ソートはExcelで行う。
	var eventrank ShowroomDBlib.EventRank
	no := len(last_eventranking)
	for _, ner := range new_eventranking {

		if ner.Status != 1 {
			eventrank.Order = no
			no++
			eventrank.Listner = ner.Listner
			eventrank.Rank = ner.Rank
			eventrank.Point = ner.Point
			eventrank.Order = ner.Order
			//	eventrank.T_LsnID = ner.Order + idx*1000
			eventrank.T_LsnID = ner.LsnID
			eventrank.LsnID = ner.LsnID
			eventrank.Incremental = -1

			incremental := ner.Point
			totalincremental += incremental
			eventrank.Incremental = incremental

			last_eventranking = append(last_eventranking, eventrank)
		}
	}

	final_eventranking = last_eventranking

	return
}

func CompareEventRanking(
	last_eventranking ShowroomDBlib.EventRanking,
	new_eventranking ShowroomDBlib.EventRanking,
	idx int,
) (ShowroomDBlib.EventRanking, int) {

	totalincremental := 0

	log.Printf("          Phase 1\n")
	//	既存のデータとリスナー名が一致するデータがあったときは既存のデータを更新する。
	ncol := 1
	msg := ""
	for j := 0; j < len(last_eventranking); j++ {
		for i := 0; i < len(new_eventranking); i++ {
			if new_eventranking[i].Status == 1 {
				continue
			}
			if new_eventranking[i].Listner == last_eventranking[j].Listner {
				if new_eventranking[i].Point >= last_eventranking[j].Point {
					if last_eventranking[j].Point != -1 {
						incremental := new_eventranking[i].Point - last_eventranking[j].Point
						totalincremental += incremental
						last_eventranking[j].Incremental = incremental
					} else {
						last_eventranking[j].Incremental = -1
					}
					if new_eventranking[i].LsnID != 0 {
						last_eventranking[j].LsnID = new_eventranking[i].LsnID
					}
					last_eventranking[j].Rank = new_eventranking[i].Rank
					last_eventranking[j].Point = new_eventranking[i].Point
					last_eventranking[j].Order = new_eventranking[i].Order
					last_eventranking[j].Lastname = ""
					new_eventranking[i].Status = 1
					last_eventranking[j].Status = 1
					msg = msg + fmt.Sprintf("%3d/%3d  ", j, i)
					if ncol == 10 {
						log.Printf("%s\n", msg)
						ncol = 1
						msg = ""
					} else {
						ncol++
					}
					break
				}
			}
		}
	}
	if msg != "" {
		log.Printf("%s\n", msg)
	}

	log.Printf("          Phase 2\n")

	phase2 := func() {
		log.Printf("     vvvvv     Phase 2\n")

		//	現在のポイント以上のリスナーが一人しかいないなら同一人物のはず
	Outerloop:
		for j := 0; j < len(last_eventranking); j++ {
			if last_eventranking[j].Status == 1 {
				continue
			}
			noasgn := -1
			for i := 0; i < len(new_eventranking); i++ {
				if new_eventranking[i].Status == 1 {
					//	すでに突き合わせが終わったものは対象にしない。
					continue
				}
				if new_eventranking[i].Point < 0 {
					//	いったんランクキング表外に出たものは突き合わせの対象としない。
					continue
				}
				if new_eventranking[i].Point < last_eventranking[j].Point {
					break
				}

				if noasgn != -1 {
					//	現在のポイント以上のリスナーが複数人いるとき
					//	ここで処理を完全やめてしまうのは last_eventranking がソートしてあることが前提
					//	ソートされていないのであれば単なるbreakにすべき
					break Outerloop
				} else {
					//	現在のポイント以上のはじめてのリスナー
					noasgn = i
				}
			}
			if noasgn != -1 {
				//	現在のポイント以上のリスナーが一人しかいなかった
				if last_eventranking[j].Point != -1 {
					incremental := new_eventranking[noasgn].Point - last_eventranking[j].Point
					totalincremental += incremental
					last_eventranking[j].Incremental = incremental
				} else {
					last_eventranking[j].Incremental = -1
				}
				if new_eventranking[noasgn].LsnID != 0 {
					last_eventranking[j].LsnID = new_eventranking[noasgn].LsnID
				}
				last_eventranking[j].Rank = new_eventranking[noasgn].Rank
				last_eventranking[j].Point = new_eventranking[noasgn].Point
				last_eventranking[j].Order = new_eventranking[noasgn].Order
				new_eventranking[noasgn].Status = 1
				last_eventranking[j].Status = 1
				last_eventranking[j].Lastname = last_eventranking[j].Listner + " [2]"
				last_eventranking[j].Listner = new_eventranking[noasgn].Listner
				log.Printf("*****         【%s】 equals to 【%s】\n", new_eventranking[noasgn].Listner, last_eventranking[j].Lastname)
			}

		}
		log.Printf("     ^^^^^     Phase 2\n")
	}
	//	コメントにした理由を思い出す！
	//	phase2()

	log.Printf("          Phase 3\n")
	//	完全に一致するものがない場合は一致度が高いものを探す。
	// weighted
	wd := lsdp.Weights{Insert: 0.8, Delete: 0.8, Replace: 1.0}
	// weighted and normalized
	nd := lsdp.Normalized(wd)
	for j := 0; j < len(last_eventranking); j++ {
		if last_eventranking[j].Status == 1 {
			continue
		}
		log.Println("---------------")
		first_n := 0
		first_v := 2.0
		second_v := 2.0
		for i := 0; i < len(new_eventranking); i++ {
			if new_eventranking[i].Status == 1 {
				continue
			}
			if new_eventranking[i].Point < last_eventranking[j].Point {
				break
			}

			newlistner := new_eventranking[i].Listner
			lastlistner := last_eventranking[j].Listner
			value := nd.Distance(newlistner, lastlistner)
			log.Printf("%6.3f [%3d] 【%s】 [%3d] 【%s】\n", value, j, lastlistner, i, newlistner)
			if value < first_v {
				second_v = first_v
				first_v = value
				first_n = i
			} else if value < second_v {
				second_v = value
			}
		}

		phase3 := func(cond string, dist float64) {
			if last_eventranking[j].Point != -1 {
				incremental := new_eventranking[first_n].Point - last_eventranking[j].Point
				totalincremental += incremental
				last_eventranking[j].Incremental = incremental
			} else {
				last_eventranking[j].Incremental = -1
			}
			if new_eventranking[first_n].LsnID != 0 {
				last_eventranking[j].LsnID = new_eventranking[first_n].LsnID
			}
			last_eventranking[j].Rank = new_eventranking[first_n].Rank
			last_eventranking[j].Point = new_eventranking[first_n].Point
			last_eventranking[j].Order = new_eventranking[first_n].Order
			new_eventranking[first_n].Status = 1
			last_eventranking[j].Status = 1
			last_eventranking[j].Lastname = last_eventranking[j].Listner + " [" + cond + fmt.Sprintf("%6.3f", dist) + "]"
			last_eventranking[j].Listner = new_eventranking[first_n].Listner
			log.Printf("*****         【%s】 equals to 【%s】\n", last_eventranking[j].Lastname, new_eventranking[first_n].Listner)
		}

		switch {
		//	case first_v < 0.72:	//	この数値は大きすぎると思われる。0.6を超えて一致と判断されるものはあやしいものが多かった（2022-03-23)
		case first_v < 0.62:
			//	一致度が高い
			phase3("3A", first_v)
		case second_v < 1.1 && second_v-first_v > 0.2:
			//	一致度が他に比較して高い
			phase3("3B", first_v)
		case first_v < 1.1 && second_v > 1.1 &&
			last_eventranking[j].Point != -1 &&
			(j == len(last_eventranking)-1 || last_eventranking[j].Point != last_eventranking[j+1].Point):
			//	一致度のチェック対象が一つしかない
			//	ここで last_eventranking[j].Point != last_eventranking[j+1].Point の条件が成り立たないことはありえないはずだが...
			phase3("3C", first_v)
		default:
			//	同一と思われるデータがみつからなかった。
			last_eventranking[j].Point = -1
			last_eventranking[j].Incremental = -1
			last_eventranking[j].Status = -1
			last_eventranking[j].Order = 999
			last_eventranking[j].Lastname = ""
			log.Printf("*****         【%s】  not found.\n", last_eventranking[j].Listner)
		}

	}

	phase2()

	log.Printf("          Phase 4\n")

	//	既存のランキングになかったリスナーを既存のランキングに追加する。
	//	ソートはしない。ソートするとExcelにあるデータと整合性がとれなくなる。
	//	つまり、ソートはExcelで行う。
	var eventrank ShowroomDBlib.EventRank
	no := len(last_eventranking)
	for i := 0; i < len(new_eventranking); i++ {
		if new_eventranking[i].Status != 1 {
			eventrank.Order = no
			no++
			eventrank.Listner = new_eventranking[i].Listner
			eventrank.Rank = new_eventranking[i].Rank
			eventrank.Point = new_eventranking[i].Point
			eventrank.Order = new_eventranking[i].Order
			//	eventrank.T_LsnID = new_eventranking[i].Order + idx*1000
			eventrank.T_LsnID = new_eventranking[i].LsnID
			if new_eventranking[i].LsnID != 0 {
				eventrank.LsnID = new_eventranking[i].LsnID
			}

			eventrank.Incremental = -1

			incremental := new_eventranking[i].Point
			totalincremental += incremental
			eventrank.Incremental = incremental

			last_eventranking = append(last_eventranking, eventrank)
		}
	}

	return last_eventranking, totalincremental
}
*/
// シャットダウンに関連する状態をまとめた構造体
type AppShutdownManager struct {
	Ctx    context.Context
	Cancel context.CancelFunc // トップレベルでのみ使うことが多いが、構造体に含めることも可能
	Wg     *sync.WaitGroup
	// 他のリソース（DB接続など）を含めることも可能
	// DB *gorp.DbMap // 例
}

// CloseResources はシャットダウン処理とリソース解放を行います。
// defer で呼び出されることを想定しています。
func (sm *AppShutdownManager) CloseResources() {
	log.Println("Closing resources...")

	// コンテキストをキャンセルし、新しいgoroutineの起動を停止
	// シグナル受信などで既に呼ばれている可能性もあるが、冪等なので問題ない
	sm.Cancel()
	log.Println("Context cancelled.")

	// WaitGroupの完了を待つのは、通常main関数で行います。
	// ここでWaitすると、CloseResourcesがブロックされてしまい、
	// main関数がWaitする前にリソース解放が完了しない可能性があります。
	// そのため、Waitはmain関数に任せるのが一般的です。

	// 他のリソース解放処理
	// if sm.DB != nil {
	//      sm.DB.Db.Close() // gorpのDB接続をクローズ
	//      log.Println("Database connection closed.")
	// }
	// 他のリソース解放処理
	log.Println("Resources closed.")
}

// -------------------------------------------

func main() {

	logfilename := "SRCntrb" + "_" + version + "_" + srapi.Version + "_" + srdblib.Version + "_" + exsrapi.Version + "_" + time.Now().Format("20060102") + ".txt"
	logfile, err := os.OpenFile(logfilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic("cannnot open logfile: " + logfilename + err.Error())
	}
	defer logfile.Close()
	log.SetOutput(logfile)
	//	log.SetOutput(io.MultiWriter(logfile, os.Stdout))

	log.Printf("\n")
	log.Printf("\n")
	log.Printf("************************ %s *********************\n", logfilename)

	// dbconfig := &srdblib.DBConfig{}
	_, err = srdblib.OpenDb("DBConfig.yml")
	if err != nil {
		log.Printf("srdblib.OpenDB returned err = %s\n", err.Error())
		return
	}
	defer srdblib.Db.Close()

	dial := gorp.MySQLDialect{Engine: "InnoDB", Encoding: "utf8mb4"}
	srdblib.Dbmap = &gorp.DbMap{Db: srdblib.Db, Dialect: dial, ExpandSliceArgs: true}
	srdblib.Dbmap.AddTableWithName(srdblib.Event{}, "event").SetKeys(false, "Eventid")
	srdblib.Dbmap.AddTableWithName(srdblib.Eventrank{}, "eventrank").SetKeys(false, "Eventid", "Userid", "Ts", "T_lsnid")
	srdblib.Dbmap.AddTableWithName(srdblib.Timetable{}, "timetable").SetKeys(false, "Eventid", "Userid", "Sampletm1")

	// -------------------------------------

	// 1. シグナル通知用のチャネルを作成
	// バッファリングされたチャネルにすることで、シグナル受信と処理の間に少し余裕を持たせます。
	sigCh := make(chan os.Signal, 1)
	// SIGINT (Ctrl+C) と SIGTERM を補足するように設定
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 2. 新しいgoroutineの起動を制御するためのコンテキスト
	// context.WithCancel() でキャンセル可能なコンテキストを作成します。
	ctx, cancel := context.WithCancel(context.Background())
	// main関数が終了する際に確実にcancel()が呼ばれるようにdeferで設定
	// (シグナル受信時にもcancel()を呼びますが、二重呼び出しは問題ありません)
	// defer cancel() // (sm *AppShutdownManager) CloseResources()で呼び出されるので、ここでは不要)

	// 3. 実行中のgoroutineを追跡するための WaitGroup
	var wg sync.WaitGroup

	// ShutdownManager インスタンスを作成
	// DB接続などの初期化もここで行う
	sm := &AppShutdownManager{
		Ctx:    ctx,
		Cancel: cancel,
		Wg:     &wg,
		// DB: initDB(), // 例
	}
	// main関数が終了する際に、リソース解放処理を確実に実行
	// これにより、シグナル受信、エラー終了、正常終了のいずれの場合でも呼ばれる
	defer sm.CloseResources()

	// -------------------------------------

	var dt time.Time
	var nday int

	sdate := os.Getenv("STARTDATE")
	if sdate == "" {
		// 環境変数が設定されていない場合は、現在の日付を使用
		dt = time.Now().Add(9 * time.Hour).Truncate(24 * time.Hour).Add(-9 * time.Hour)
	} else {
		// 環境変数から日付を取得
		var err error
		dt, err = time.Parse("2006-01-02", sdate)
		if err != nil {
			log.Printf("環境変数 STARTDATE の値が不正です: %s\n", sdate)
			log.Printf("現在の日付を使用します。\n")
			dt = time.Now()
		}
	}

	snday := os.Getenv("NDAY")
	if snday == "" {
		// 環境変数が設定されていない場合は、デフォルト値を使用
		nday = 7 // デフォルトで7日間
	} else {
		// 環境変数から日数を取得
		var err error
		nday, err = strconv.Atoi(snday)
		if err != nil {
			log.Printf("環境変数 NDAY の値が不正です: %s\n", snday)
			log.Printf("デフォルト値 7 を使用します。\n")
			nday = 7 // デフォルトで7日間
		}
	}

	log.Printf("開始日: %s\n", dt.Format("2006-01-02"))
	log.Printf("日数: %d\n", nday)

	// WaitGroupの完了を通知するためのチャネル
	// このチャネルは、sm.Wg に追加された全てのgoroutineが完了したときに閉じられる
	allGoroutinesDoneCh := make(chan struct{})

	// WaitGroupの完了を待つ別のgoroutine
	// sm.Wg.Wait() はブロッキングなので、別のgoroutineで実行する
	go func() {
		sm.Wg.Wait()
		close(allGoroutinesDoneCh) // WaitGroupが完了したらチャネルを閉じる
		log.Println("WaitGroup: 全てのgoroutineが完了しました")
	}()

	sm.Wg.Add(1)
	go SetContributionPoint(sm, dt, nday)

	log.Println("メイン: シグナル受信 または 全てのgoroutine完了 を待機中...")

	// シグナル受信 または 全てのgoroutine完了 のいずれかを待つ
	select {
	case sig := <-sigCh:
		log.Printf("メイン: シグナル %v を受信しました。終了処理を開始します。", sig)
		// シグナル受信時は、他のgoroutineに終了を通知するためにContextをキャンセル
		cancel()
		// Contextキャンセル後、他のgoroutineが終了するのを待つ
		// SetContributionPointがContextで中断可能なら、ここでWaitGroup完了を待つ
		<-allGoroutinesDoneCh // WaitGroupが完了するのを待つ
	case <-allGoroutinesDoneCh:
		log.Println("メイン: 全てのgoroutineが完了しました。終了処理を開始します。")
		// goroutineが自然に全て完了した場合、Contextキャンセルは不要かもしれないが、
		// 安全のため呼んでおくと良い場合もある。今回は呼ばなくても良い。
		// cancel()
	}

	// シグナル受信をトリガーとして、ShutdownManager経由でキャンセルを呼び出す
	// これにより、Contextを監視しているgoroutineが終了を開始する
	// deferされたCloseResourcesでもCancelは呼ばれるが、シグナル受信時に
	// 即座にキャンセルをトリガーしたい場合はここで明示的に呼ぶ
	// (CloseResources内でCancelを呼ぶ設計の場合は、ここでの明示的な呼び出しは不要)
	// 今回はCloseResources内でCancelを呼ぶ設計なので、ここはコメントアウト
	// sm.Cancel()
	log.Println("シャットダウン処理を開始します。")

	// ShutdownManager経由でWaitを呼び、実行中のすべてのgoroutineが終了するのを待つ
	// Contextがキャンセルされた後、すべてのワーカーgoroutineがDone()を呼ぶのを待つ
	log.Println("実行中のすべてのgoroutineが終了するのを待っています...")
	sm.Wg.Wait()

	// Wait()から戻ったら、すべてのgoroutineが終了したことになります。
	// main関数が終了するため、defer sm.CloseResources() が呼ばれ、
	// リソース解放処理が行われます。
	log.Println("すべてのgoroutineが終了しました。")
	// main関数が終了すると、deferが実行され、プログラムが終了します。

}
