package main

import (
	// "fmt"
	"fmt"
	"log"

	// "github.com/Chouette2100/srapi/v2"
	"github.com/Chouette2100/srapi/v2"
	"github.com/Chouette2100/srdblib/v2"
	"github.com/go-gorp/gorp"

	// "strings"
	"net/http"
	"time"
)

func SetContributionPoint(
	sm *AppShutdownManager,
	dt time.Time,
	nday int,
) (
	err error,
) {

	defer sm.Wg.Done()

	// ndayが0の場合は、現在時から1時間前までに終わったイベントを対象とする
	bnow := false
	if nday == 0 {
		bnow = true
		nday = 1
	}

	for i := 0; i < nday; i++ {
		// 指定された日付範囲について繰り返す
		select {
		case <-sm.Ctx.Done():
			log.Println("SetContributionPoint: Context cancelled, exiting.")
			return nil
		default:
			// Contextはまだ有効
		}
		//	日付をセット
		ts := dt.AddDate(0, 0, i)
		te := ts.AddDate(0, 0, 1)
		if bnow {
			te = time.Now()
			ts = te.Add(-1 * time.Hour) // 1時間前までに終わったイベントを対象とする
		}
		// 指定された日にちに終わるイベントの一覧を取得する。
		log.Printf("SetContributionPoint: Fetching events between %s and %s", ts.Format("2006-01-02 15:04"), te.Format("2006-01-02 15:04"))

		evlist := &[]srdblib.Event{}
		sqlst := "select * from event where endtime between ? and ? "
		_, err = srdblib.Dbmap.Select(evlist, sqlst, ts, te)
		if err != nil {
			err = fmt.Errorf("SetContributionPoint: Error fetching events between %s and %s: %v", ts, te, err)
			return
		}

		for _, ev := range *evlist {
			// すべてのイベントに対して繰り返す
			log.Printf("SetContributionPoint: Processing event %s with ID %s", ev.Event_name, ev.Eventid)

			// if ev.Eventid != "tifdedebut2025_f?block_id=37402" {
			// 	continue
			// }
			select {
			case <-sm.Ctx.Done():
				log.Println("SetContributionPoint: Context cancelled, exiting.")
				return nil
			default:
				// Contextはまだ有効
			}

			// イベント参加ユーザーの一覧を作る
			ulist := &[]srdblib.Eventuser{}
			sqlst = "SELECT * FROM eventuser WHERE eventid = ? "
			_, err = srdblib.Dbmap.Select(ulist, sqlst, ev.Eventid)
			if err != nil {
				log.Printf("SetContributionPoint: Error fetching users for event %s: %v", ev.Eventid, err)
				continue
			}
			// イベント参加ユーザーの一覧に対して繰り返す
			tnow := time.Now().Truncate(time.Second)
			for _, u := range *ulist {
				// log.Printf("SetContributionPoint: Processing user %d for event %s", u.Userno, ev.Eventid)
				// if u.Userno != 548711 {
				// 	continue
				// }
				select {
				case <-sm.Ctx.Done():
					log.Println("SetContributionPoint: Context cancelled, exiting.")
					return nil
				default:
					// Contextはまだ有効
				}

				// ユーザーの貢献ポイントを取得する
				var cr *srapi.Contribution_ranking
				cr, err = srapi.ApiEventContribution_ranking(&http.Client{}, ev.Ieventid, u.Userno)
				if err != nil || cr == nil || len(cr.Ranking) == 0 {
					if err != nil {
						log.Printf("SetContributionPoint: Error fetching contribution ranking for user %d in event %s: %v", u.Userno, ev.Eventid, err)
					} else {
						log.Printf("SetContributionPoint: No contribution ranking found for user %d in event %s", u.Userno, ev.Eventid)
					}
					log.Println(err)
					continue
				}

				// DBの貢献ポイントデータの最後の日時のデータを取得する
				er := make([]srdblib.Eventrank, 0)
				sqlst = "select * from eventrank "
				sqlst += " where eventid = ? and userid = ? "
				sqlst += " and ts = (select max(ts) from eventrank where eventid = ? and userid = ? ) "
				sqlst += " order by norder "
				_, err = srdblib.Dbmap.Select(&er, sqlst, ev.Eventid, u.Userno, ev.Eventid, u.Userno)
				if err != nil {
					err = fmt.Errorf("SetContributionPoint: Error fetching event rank for user %d in event %s: %v", u.Userno, ev.Eventid, err)
					log.Println(err)
					continue
				}

				lsnmap := make(map[int]int)
				for j, erow := range er {
					// 取得した貢献ポイントのリスナーIDをマップに格納
					lsnmap[erow.Lsnid] = j
				}

				beq := true
				for _, pcr := range cr.Ranking {
					// 取得した貢献ポイントのリスナーIDをマップに格納
					if _, exists := lsnmap[pcr.UserID]; !exists {
						beq = false
						log.Printf("SetContributionPoint: User %d in event %s not found in existing ranks", pcr.UserID, ev.Eventid)
						break
					}
					if pcr.Point != er[lsnmap[pcr.UserID]].Point {
						beq = false
						log.Printf("SetContributionPoint: Point mismatch for user %d in event %s: expected %d, got %d",
							u.Userno, ev.Eventid, er[lsnmap[pcr.UserID]].Point, pcr.Point)
						break
					}
				}
				if beq {
					continue
				}

				// DBに貢献ポイントがない、または取得した貢献ポイントがDBにある貢献ポイントが一致しない
				// 取得した貢献ポイントをDBに格納する
				var tx *gorp.Transaction
				tx, err = srdblib.Dbmap.Begin()
				if err != nil {
					err = fmt.Errorf("SetContributionPoint: Error starting transaction for user %d in event %s: %v", u.Userno, ev.Eventid, err)
					log.Println(err)
					continue
				}
				defer tx.Rollback() // トランザクションのロールバックをデファード

				for i, pcr := range cr.Ranking {
					ner := srdblib.Eventrank{
						Eventid:   ev.Eventid,
						Userid:    u.Userno,
						Ts:        tnow,
						Listner:   pcr.Name,
						Lastname:  pcr.Name,
						Lsnid:     pcr.UserID,
						T_lsnid:   pcr.UserID,
						Norder:    i + 1,
						Nrank:     pcr.Rank,
						Point:     pcr.Point,
						Increment: pcr.Point,
						Status:    0,
					}

					// もしリスナーIDがマップに存在する場合、IncrementをPointの差分に設定する
					if idx, exists := lsnmap[pcr.UserID]; exists {
						// 既存のリスナーIDが存在する場合、Incrementを計算
						if er[idx].Point > 0 {
							// 既存のポイントが0より大きい場合、Incrementは現在のポイントから既存のポイントを引いた値
							ner.Increment = pcr.Point - er[idx].Point
						} else {
							// 既存のポイントが0以下の場合、Incrementは現在のポイント
							ner.Increment = -1
						}
					} else {
						// 新しいリスナーIDの場合、IncrementはそのままPoint
						ner.Increment = pcr.Point
					}

					// DBに格納
					err = srdblib.Dbmap.Insert(&ner)
					if err != nil {
						err = fmt.Errorf("SetContributionPoint: Error inserting event rank for user %d in event %s: %v", u.Userno, ev.Eventid, err)
						log.Println(err)
						continue
					}
					// log.Printf("SetContributionPoint: Inserted event rank for user %d in event %s with point %d", u.Userno, ev.Eventid, cr.Ranking[i].Point)
				}
				tt := srdblib.Timetable{
					Eventid:     ev.Eventid,
					Userid:      u.Userno,
					Sampletm1:   tnow,
					Sampletm2:   tnow,
					Stime:       ev.Endtime,
					Etime:       ev.Endtime,
					Target:      -1,
					Totalpoint:  0,
					Earnedpoint: 0,
					Status:      1,
				}
				err = srdblib.Dbmap.Insert(&tt)
				if err != nil {
					err = fmt.Errorf("SetContributionPoint: Error inserting timetable for user %d in event %s: %v", u.Userno, ev.Eventid, err)
					log.Println(err)
					continue
				}
				tx.Commit() // トランザクションをコミット

				log.Printf("SetContributionPoint: Inserted timetable for user %d in event %s", u.Userno, ev.Eventid)

				// time.Sleep(10 * time.Second)
				select {
				case <-sm.Ctx.Done():
					log.Println("Wait cancelled by context.")
					// キャンセルされた場合の処理 (例: ループを抜ける)
					return
				case <-time.After(1 * time.Second):
					log.Println("Wait finished.")
					// 待機が完了した場合の処理
					continue
				}
			}

		}

	}

	return
}
