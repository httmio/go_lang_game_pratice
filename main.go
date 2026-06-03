package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/lesismal/nbio/nbhttp"
	"github.com/lesismal/nbio/nbhttp/websocket"
)

// --- 資料結構定義 ---

// Tree 代表遊戲中的一棵樹
type Tree struct {
	ID string `json:"id"`
	HP int    `json:"hp"`
}

// ClientAction 代表玩家傳上來的動作
type ClientAction struct {
	Action string `json:"action"`  // 例如: "chop"
	TreeID string `json:"tree_id"` // 想要砍的樹 ID
}

// GameEvent 代表伺服器廣播出去的事件
type GameEvent struct {
	Event     string `json:"event"`      // 例如: "tree_damaged"
	PlayerID  string `json:"player_id"`  // 誰砍的
	TreeID    string `json:"tree_id"`    // 哪棵樹被砍
	CurrentHP int    `json:"current_hp"` // 剩餘血量
}

// --- 遊戲狀態管理 (Game State) ---
// 新增一個結構體，用來初始化世界狀態
type InitEvent struct {
	Event string           `json:"event"` // 固定為 "init"
	Trees map[string]*Tree `json:"trees"` // 所有的樹木資料
}

var (
	// 儲存地圖上所有的樹 (TreeID -> Tree)
	worldTrees = map[string]*Tree{
		"tree_1": {ID: "tree_1", HP: 100},
		"tree_2": {ID: "tree_2", HP: 100},
		"tree_3": {ID: "tree_3", HP: 100},
	}
	stateMu sync.Mutex // 保護世界狀態的鎖

	// 管理所有在線玩家的連線 (Conn -> PlayerID)
	onlinePlayers = map[*websocket.Conn]string{}
	playerMu      sync.Mutex
)

// 廣播訊息給所有在線玩家
func broadcast(msgType websocket.MessageType, data []byte) {
	playerMu.Lock()
	defer playerMu.Unlock()
	for conn := range onlinePlayers {
		conn.WriteMessage(msgType, data)
	}
}

func main() {
	upgrader := websocket.NewUpgrader()
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	// 1. 當有玩家連線進來
	upgrader.OnOpen(func(c *websocket.Conn) {
		playerMu.Lock()
		// 簡單用連線指針的地址當作臨時 Player ID
		playerID := fmt.Sprintf("player_%p", c)
		onlinePlayers[c] = playerID
		playerMu.Unlock()

		fmt.Printf("玩家 %s 進入了遊戲\n", playerID)

		stateMu.Lock()
		initEvent := InitEvent{
			Event: "init",
			Trees: worldTrees, // 把當前伺服器記憶體裡樹木的血量打包
		}
		stateMu.Unlock()
		initBytes, _ := json.Marshal(initEvent)
		// 只寫入給這個剛連線的 c，不廣播給其他人
		c.WriteMessage(websocket.TextMessage, initBytes)

	})
	upgrader.CheckOrigin = func(r *http.Request) bool {
		return true // 允許瀏覽器跨網域連線測試
	}
	// 2. 當玩家斷開連線
	upgrader.OnClose(func(c *websocket.Conn, err error) {
		playerMu.Lock()
		playerID := onlinePlayers[c]
		delete(onlinePlayers, c)
		playerMu.Unlock()

		fmt.Printf("玩家 %s 離開了遊戲\n", playerID)
	})

	// 3. 當收到玩家的「砍樹」指令
	upgrader.OnMessage(func(c *websocket.Conn, messageType websocket.MessageType, data []byte) {
		// 找到是哪個玩家發出的
		playerMu.Lock()
		playerID := onlinePlayers[c]
		playerMu.Unlock()

		// 解析指令
		var action ClientAction
		if err := json.Unmarshal(data, &action); err != nil {
			return
		}

		// 如果動作是砍樹
		if action.Action == "chop" {
			stateMu.Lock()
			tree, exists := worldTrees[action.TreeID]

			if exists && tree.HP > 0 {
				tree.HP -= 10 // 每砍一下扣 10 血
				if tree.HP < 0 {
					tree.HP = 0
				}
				fmt.Printf("玩家 %s 砍了 %s, 剩餘血量: %d\n", playerID, tree.ID, tree.HP)

				// 準備廣播事件
				event := GameEvent{
					Event:     "tree_damaged",
					PlayerID:  playerID,
					TreeID:    tree.ID,
					CurrentHP: tree.HP,
				}
				stateMu.Unlock()

				// 將結果廣播給所有人
				responseBytes, _ := json.Marshal(event)
				broadcast(messageType, responseBytes)
			} else {
				stateMu.Unlock()
			}
		}
	})

	// 啟動伺服器邏輯
	mux := http.NewServeMux()
	mux.HandleFunc("/game", func(w http.ResponseWriter, r *http.Request) {
		_, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html") // 把同目錄下的 index.html 丟給瀏覽器
	})

	server := nbhttp.NewServer(nbhttp.Config{
		Network: "tcp",
		Addrs:   []string{":8080"},
		Handler: mux,
	})

	if err := server.Start(); err != nil {
		log.Fatalf("啟動失敗: %v", err)
	}
	defer server.Stop()

	fmt.Println("遊戲伺服器已啟動，監聽 :8080/game")
	for {
		time.Sleep(time.Second)
	}
}
