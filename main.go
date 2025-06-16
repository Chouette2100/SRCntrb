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

	"github.com/go-gorp/gorp"

	"github.com/Chouette2100/exsrapi/v2"
	"github.com/Chouette2100/srapi/v2"
	"github.com/Chouette2100/srdblib/v2"
)

/*
00AA00 新規作成
00AB00 貢献ランキングの書き込みをユーザー単位のトランザクションとする
00AB01 貢献ランキングの取得でエラーが置きたとき、または貢献ランキングが存在しない場合はスキップする
00AC00 nday==0 のときは、現在時から1時間前までに終わったイベントを対象とする。
*/

const version = "00AC00"

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
			// log.Printf("デフォルト値 7 を使用します。\n")
			// nday = 7 // デフォルトで7日間
			return
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
