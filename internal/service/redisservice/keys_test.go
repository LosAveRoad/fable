package redisservice

import "testing"

func TestKeyBuildersAreStableAndSymmetric(t *testing.T) {
	if got := SessionPairKey("U002", "U001"); got != "fable:v1:session:U001:U002" {
		t.Fatalf("session key = %q", got)
	}
	if MessageListKey("U002", "U001") != MessageListKey("U001", "U002") {
		t.Fatal("message pair key is not symmetric")
	}
	if got := GroupMessageListKey("G001"); got != "fable:v1:group_messagelist:G001" {
		t.Fatalf("group message key = %q", got)
	}
}
