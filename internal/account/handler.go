package account

import (
	"kirfeed/internal/apierror"
	"net/http"

	"github.com/gin-gonic/gin"
)

type AccountHandler struct {
	accountService *AccountService
}

func NewAccountHandler(accountService *AccountService) *AccountHandler {
	return &AccountHandler{
		accountService: accountService,
	}
}

func (h *AccountHandler) Register(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(apierror.ErrValidation), gin.H{"error": err.Error()})
		return
	}
	if err := h.accountService.CreateAccount(c.Request.Context(), &Account{
		Username: req.Username,
		Password: req.Password,
	}); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"message": "account created"})
}

func (h *AccountHandler) Login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(apierror.ErrValidation), gin.H{"error": err.Error()})
		return
	}
	account, err := h.accountService.FindByUsername(c.Request.Context(), req.Username)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(apierror.ErrUnauthorized), gin.H{"error": "invalid credentials"})
		return
	}
	accessToken, refreshToken, err := h.accountService.Login(c.Request.Context(), req.Username, req.Password)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(apierror.ErrUnauthorized), gin.H{"error": "invalid credentials"})
		return
	}
	c.JSON(http.StatusOK, LoginResponse{Token: accessToken, RefreshToken: refreshToken, AccountID: account.ID, Username: account.Username})
}

func (h *AccountHandler) Refresh(c *gin.Context) {
	var req RefreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(apierror.ErrValidation), gin.H{"error": err.Error()})
		return
	}
	newToken, accountID, username, err := h.accountService.RefreshAccessToken(c.Request.Context(), req.RefreshToken)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(apierror.ErrUnauthorized), gin.H{"error": "invalid refresh token"})
		return
	}
	c.JSON(http.StatusOK, LoginResponse{Token: newToken, RefreshToken: req.RefreshToken, AccountID: accountID, Username: username})
}

func (h *AccountHandler) Logout(c *gin.Context) {
	accountID, err := getAccountID(c)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	if err := h.accountService.Logout(c.Request.Context(), accountID); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "account logged out"})
}

func getAccountID(c *gin.Context) (uint, error) {
	value, exists := c.Get("accountID")
	if !exists {
		return 0, apierror.ErrUnauthorized
	}
	id, ok := value.(uint)
	if !ok {
		return 0, apierror.ErrUnauthorized
	}
	return id, nil
}
