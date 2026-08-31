package gormservice

import (
	"context"
	"fmt"
	"mychat/internal/config"
	"mychat/internal/dao"
	"mychat/internal/dto/request"
	"mychat/internal/dto/response"
	"mychat/internal/model"
	"mychat/internal/service/redisservice"
	"regexp"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

func isPhone(phone string) bool {
	reg := regexp.MustCompile(`^1[3-9]\d{9}$`)
	return reg.MatchString(phone)
}
func passwordHash(password string) string {
	hash, _ := bcrypt.GenerateFromPassword(
		[]byte(password),
		bcrypt.DefaultCost,
	)
	return string(hash)
}

func Register(r *request.RegisterRequest) (*response.RegisterResponse, error) {
	if isPhone(r.Telephone) == false {
		return nil, ErrInvalidRegister
	}

	var count int64

	dao.GormDB.
		Model(&model.UserInfo{}).
		Where("telephone = ?", r.Telephone).
		Count(&count)

	if count > 0 {
		return nil, ErrTelephoneExists
	}

	count = 0

	dao.GormDB.
		Model((&model.UserInfo{})).
		Where("nickname = ?", r.Nickname).
		Count(&count)

	if count > 0 {
		return nil, ErrUsernameExists
	}

	uuid := fmt.Sprintf("U%s", uuid.NewString())

	user := model.UserInfo{
		UUID:         uuid,
		Telephone:    r.Telephone,
		Nickname:     r.Nickname,
		PasswordHash: passwordHash(r.Password),
	}

	if err := dao.GormDB.Create(&user).Error; err != nil {
		return nil, ErrCreateUserFailed
	}

	return &response.RegisterResponse{
		UUID:      uuid,
		Nickname:  r.Nickname,
		Telephone: r.Telephone,
	}, nil
}

var jwtKey []byte

func InitJWT(JWTConfig config.JWTConfig) {
	jwtKey = JWTConfig.Secret
}

func generateToken(userUUID string) (string, error) {

	claims := jwt.MapClaims{
		"user_uuid": userUUID,
		"exp":       time.Now().Add(time.Hour * 24).Unix(),
	}

	token := jwt.NewWithClaims(
		jwt.SigningMethodHS256,
		claims,
	)

	return token.SignedString(jwtKey)
}

func Login(r *request.LoginRequest) (*response.LoginResponse, error) {

	var count int64
	dao.GormDB.Model(model.UserInfo{}).Where("telephone = ?", r.Telephone).Count(&count)

	if count == 0 {
		return nil, ErrUserNotFound
	}

	var user model.UserInfo
	if err := dao.GormDB.Where("telephone = ?", r.Telephone).First(&user).Error; err != nil {
		return nil, ErrLoginFailed
	}

	if err := bcrypt.CompareHashAndPassword(
		[]byte(user.PasswordHash),
		[]byte(r.Password),
	); err != nil {
		return nil, ErrInvalidPassword
	}

	token, err := generateToken(user.UUID)
	if err != nil {
		return nil, ErrLoginFailed
	}

	return &response.LoginResponse{
		UUID:     user.UUID,
		Nickname: user.Nickname,
		Token:    token,
	}, nil
}

func GetUserInfo(r *request.GetUserInfoRequest, uuid_any any) (*response.GetUserInfoResponse, error) {
	uuid, ok := uuid_any.(string)
	if !ok {
		return nil, ErrInvalidUUID
	}
	var cached response.GetUserInfoResponse
	if err := redisservice.GetJSON(context.Background(), redisservice.UserInfoKey(uuid), &cached); err == nil {
		return &cached, nil
	}

	var user model.UserInfo

	if err := dao.GormDB.Where("uuid = ?", uuid).First(&user).Error; err != nil {
		return nil, ErrForbidden
	}

	if user.UUID != uuid {
		return nil, ErrUserAccessDenied
	}

	result := response.GetUserInfoResponse{
		UUID:      user.UUID,
		Nickname:  user.Nickname,
		Telephone: user.Telephone,
	}
	_ = redisservice.SetJSON(context.Background(), redisservice.UserInfoKey(uuid), result, redisservice.DefaultCacheTTL)
	return &result, nil
}
