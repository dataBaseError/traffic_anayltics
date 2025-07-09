package main

import (
	"math/rand"
	"time"
)

func generateRandomSites(n int) []string {
	sites := []string{"google.com", "youtube.com", "facebook.com", "instagram.com", "chatgpt.com", "x.com", "whatsapp.com", "wikipedia.org", "reddit.com", "yahoo.co.jp", "yahoo.com", "yandex.ru", "tiktok.com", "amazon.com", "baidu.com", "bet.br", "linkedin.com", "pornhub.com", "naver.com", "live.com", "netflix.com", "temu.com", "dzen.ru", "bing.com", "office.com", "bilibili.com", "pinterest.com", "xvideos.com", "microsoft.com", "twitch.tv", "xhamster.com", "vk.com", "mail.ru", "news.yahoo.co.jp", "sharepoint.com", "weather.com", "fandom.com", "canva.com", "samsung.com", "globo.com", "t.me", "xnxx.com", "duckduckgo.com", "stripchat.com", "roblox.com", "openai.com", "docomo.ne.jp", "nytimes.com", "ebay.com", "aliexpress.com"}
	return sites
}

func generateMsgs(n int, sites []string) []Msg {
	msgs := make([]Msg, n)
	now := time.Now().UnixMilli()
	for i := 0; i < n; i++ {
		msgs[i] = Msg{
			Site:   sites[rand.Intn(len(sites))],
			Id:     rand.Uint64(),
			TimeMs: now - rand.Int63n(1000*60*60*24), // within last 24 hours
		}
	}
	return msgs
}
