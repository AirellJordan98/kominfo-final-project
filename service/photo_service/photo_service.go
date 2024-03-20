package photo_service

import (
	"myGram/dto"
	"myGram/entity"
	"myGram/pkg/errs"
	"myGram/pkg/helper"
	"myGram/repository/photo_repository"
	"net/http"
)

type PhotoService interface {
	AddPhoto(userId int, photoPayload *dto.NewPhotoRequest) (*dto.GetPhotoResponse, errs.Error)
	GetPhotos() (*dto.GetPhotoResponse, errs.Error)
	UpdatePhoto(photoId int, photoPayload *dto.PhotoUpdateRequest) (*dto.GetPhotoResponse, errs.Error)
	DeletePhoto(photoId int) (*dto.GetPhotoResponse, errs.Error)
}

type photoServiceImpl struct {
	pr photo_repository.PhotoRepository
}

func NewPhotoService(photoRepository photo_repository.PhotoRepository) PhotoService {
	return &photoServiceImpl{
		pr: photoRepository,
	}
}

func (p *photoServiceImpl) AddPhoto(userId int, photoPayload *dto.NewPhotoRequest) (*dto.GetPhotoResponse, errs.Error) {

	err := helper.ValidateStruct(photoPayload)

	if err != nil {
		return nil, err
	}

	photo := &entity.Photo{
		Title:    photoPayload.Title,
		Caption:  photoPayload.Caption,
		PhotoUrl: photoPayload.PhotoUrl,
		UserId:   userId,
	}

	response, err := p.pr.AddPhoto(photo)

	if err != nil {
		return nil, err
	}

	return &dto.GetPhotoResponse{
		StatusCode: http.StatusCreated,
		Message:    "new photo successfully added",
		Data:       response,
	}, nil
}

// GetPhotos implements PhotoService.
func (p *photoServiceImpl) GetPhotos() (*dto.GetPhotoResponse, errs.Error) {

	result, err := p.pr.GetPhotos()

	if err != nil {
		if err.Status() == http.StatusNotFound {
			return nil, err
		}
		return nil, err
	}

	return &dto.GetPhotoResponse{
		StatusCode: http.StatusOK,
		Message:    "photos successfully fetched",
		Data:       result,
	}, nil
}

func (p *photoServiceImpl) UpdatePhoto(photoId int, photoPayload *dto.PhotoUpdateRequest) (*dto.GetPhotoResponse, errs.Error) {

	err := helper.ValidateStruct(photoPayload)

	if err != nil {
		return nil, err
	}

	photo := &entity.Photo{
		Title:    photoPayload.Title,
		Caption:  photoPayload.Caption,
		PhotoUrl: photoPayload.PhotoUrl,
	}

	response, err := p.pr.UpdatePhoto(photoId, photo)

	if err != nil {
		return nil, err
	}

	return &dto.GetPhotoResponse{
		StatusCode: http.StatusOK,
		Message:    "photo has been successfully updated",
		Data:       response,
	}, nil
}

func (p *photoServiceImpl) DeletePhoto(photoId int) (*dto.GetPhotoResponse, errs.Error) {

	err := p.pr.DeletePhoto(photoId)

	if err != nil {
		return nil, err
	}

	return &dto.GetPhotoResponse{
		StatusCode: http.StatusOK,
		Message:    "Your photo has been successfully deleted",
		Data:       nil,
	}, nil
}
