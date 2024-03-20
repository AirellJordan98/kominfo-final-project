package dto

import "time"

type NewPhotoRequest struct {
	Title    string `json:"title" valid:"required~Title can't be empty" example:"monday awesome"`
	PhotoUrl string `json:"photo_url" valid:"required~Photo URL can't be empty" example:"https://www.pinterest.com/pin/807973989398829161/"`
	Caption  string `json:"caption" example:"Hello I'm Monday from Weeekly, hopefully You can do this!"`
}

type PhotoUpdateRequest struct {
	Title    string `json:"title" valid:"required~Title can't be empty" example:"monday awesome"`
	PhotoUrl string `json:"photo_url" valid:"required~Photo URL can't be empty" example:"https://www.pinterest.com/pin/807973989398829161/"`
	Caption  string `json:"caption" example:"Hello I'm Monday from Weeekly, stay strong!"`
}

type PhotoResponse struct {
	Id        int       `json:"id" example:"1"`
	Title     string    `json:"title" example:"monday awesome"`
	Caption   string    `json:"caption" example:"Hello I'm Monday from weeekly, hopefully You can do this!"`
	PhotoUrl  string    `json:"photo_url" example:"https://www.pinterest.com/pin/807973989398829161/"`
	UserId    int       `json:"user_id" example:"1"`
	CreatedAt time.Time `json:"created_at" example:"2023-10-09T05:14:35.19324086+07:00"`
}

type PhotoUpdateResponse struct {
	Id        int       `json:"id" example:"1"`
	Title     string    `json:"title" example:"monday awesome"`
	Caption   string    `json:"caption" example:"Hello I'm Monday from Weeekly, stay strong!"`
	PhotoUrl  string    `json:"photo_url" example:"https://www.pinterest.com/pin/807973989398829161/"`
	UserId    int       `json:"user_id" example:"1"`
	UpdatedAt time.Time `json:"updated_at" example:"2023-10-09T05:14:35.19324086+07:00"`
}

type GetPhotoResponse struct {
	StatusCode int    `json:"status_code"`
	Message    string `json:"message"`
	Data       any    `json:"data"`
}
