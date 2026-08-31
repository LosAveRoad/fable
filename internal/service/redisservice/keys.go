package redisservice

import "sort"

const keyPrefix = "fable:v1:"

func AuthCodeKey(telephone string) string      { return keyPrefix + "auth_code:" + telephone }
func UserInfoKey(userID string) string         { return keyPrefix + "user_info:" + userID }
func ContactUserListKey(userID string) string  { return keyPrefix + "contact_user_list:" + userID }
func JoinedGroupListKey(userID string) string  { return keyPrefix + "my_joined_group_list:" + userID }
func OwnedGroupListKey(userID string) string   { return keyPrefix + "contact_mygroup_list:" + userID }
func GroupInfoKey(groupID string) string       { return keyPrefix + "group_info:" + groupID }
func GroupMemberListKey(groupID string) string { return keyPrefix + "group_memberlist:" + groupID }
func SessionListKey(userID string) string      { return keyPrefix + "session_list:" + userID }
func GroupSessionListKey(userID string) string { return keyPrefix + "group_session_list:" + userID }
func SessionPairKey(userID, peerID string) string {
	ids := []string{userID, peerID}
	sort.Strings(ids)
	return keyPrefix + "session:" + ids[0] + ":" + ids[1]
}
func MessageListKey(userID, peerID string) string {
	ids := []string{userID, peerID}
	sort.Strings(ids)
	return keyPrefix + "message_list:" + ids[0] + ":" + ids[1]
}
func GroupMessageListKey(groupID string) string { return keyPrefix + "group_messagelist:" + groupID }
