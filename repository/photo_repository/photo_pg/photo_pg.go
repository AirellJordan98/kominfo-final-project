package photo_pg

import (
	"database/sql"
	"myGram/dto"
	"myGram/entity"
	"myGram/pkg/errs"
	"myGram/repository/photo_repository"
)

type photoRepositoryImpl struct {
	db *sql.DB
}

const (
	addNewPhotoQuery = `
		INSERT INTO
			photos
				(
					title,
					caption,
					photo_url,
					user_id
				)
		VALUES
				($2, $3, $4, $1)
		RETURNING
				id, title, caption, photo_url, user_id, created_at
	`

	getUserAndPhotos = `
				SELECT
					p.id,
					p.title,
					p.caption,
					p.photo_url,
					p.user_id,
					p.created_at,
					p.updated_at,
					u.email,
					u.username
				FROM
					photos as p
				LEFT JOIN
					users AS u
				ON
					p.user_id = u.id
				ORDER BY
					p.id
				ASC
	`
	getUserAndPhotosById = `
		SELECT
			p.id,
			p.title,
			p.caption,
			p.photo_url,
			p.user_id,
			p.created_at,
			p.updated_at,
			u.email,
			u.username
		FROM
			photos as p
		LEFT JOIN
			users AS u
		ON
			p.user_id = u.id
		WHERE
				p.id = $1
		ORDER BY
			p.id
		ASC
	`

	UpdatePhotoQuery = `
		UPDATE
			photos
		SET
			title = $2,
			caption = $3,
			photo_url = $4,
			updated_at = now()
		WHERE
			id = $1
		RETURNING
				id, title, caption, photo_url, user_id, updated_at
	`

	deletePhotoById = `
		DELETE FROM
			photos
		WHERE
			id = $1		
	`
)

func NewPhotoRepository(db *sql.DB) photo_repository.PhotoRepository {
	return &photoRepositoryImpl{
		db: db,
	}
}

// AddPhoto implements photo_repository.PhotoRepository.
func (photoRepo *photoRepositoryImpl) AddPhoto(photoPayload *entity.Photo) (*dto.PhotoResponse, errs.Error) {

	tx, err := photoRepo.db.Begin()

	if err != nil {
		tx.Rollback()
		return nil, errs.NewInternalServerError("something went wrong")
	}

	row := tx.QueryRow(addNewPhotoQuery, photoPayload.UserId, photoPayload.Title, photoPayload.Caption, photoPayload.PhotoUrl)
	var photo dto.PhotoResponse

	err = row.Scan(
		&photo.Id,
		&photo.Title,
		&photo.Caption,
		&photo.PhotoUrl,
		&photo.UserId,
		&photo.CreatedAt,
	)

	if err != nil {
		tx.Rollback()
		return nil, errs.NewInternalServerError("something went wrong")
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return nil, errs.NewInternalServerError("something went wrong")
	}

	return &photo, nil
}

// GetPhotos implements photo_repository.PhotoRepository.
func (photoRepo *photoRepositoryImpl) GetPhotos() ([]photo_repository.PhotoUserMapped, errs.Error) {

	photosUser := []photo_repository.PhotoUser{}
	rows, err := photoRepo.db.Query(getUserAndPhotos)

	if err != nil {
		return nil, errs.NewInternalServerError("something went wrong")
	}

	for rows.Next() {
		photoUser := photo_repository.PhotoUser{}

		err = rows.Scan(
			&photoUser.Photo.Id,
			&photoUser.Photo.Title,
			&photoUser.Photo.Caption,
			&photoUser.Photo.PhotoUrl,
			&photoUser.Photo.UserId,
			&photoUser.Photo.CreatedAt,
			&photoUser.Photo.UpdatedAt,
			&photoUser.User.Email,
			&photoUser.User.Username,
		)

		if err != nil {
			if err == sql.ErrNoRows {
				return nil, errs.NewNotFoundError("photos not found")
			}
			return nil, errs.NewInternalServerError("something went wrong")
		}

		photosUser = append(photosUser, photoUser)
	}

	result := photo_repository.PhotoUserMapped{}
	return result.HandleMappingPhotoWithUser(photosUser), nil
}

// GetPhotoId implements photo_repository.PhotoRepository.
func (photoRepo *photoRepositoryImpl) GetPhotoId(photoId int) (*photo_repository.PhotoUserMapped, errs.Error) {

	photoUser := photo_repository.PhotoUser{}

	row := photoRepo.db.QueryRow(getUserAndPhotosById, photoId)
	err := row.Scan(
		&photoUser.Photo.Id,
		&photoUser.Photo.Title,
		&photoUser.Photo.Caption,
		&photoUser.Photo.PhotoUrl,
		&photoUser.Photo.UserId,
		&photoUser.Photo.CreatedAt,
		&photoUser.Photo.UpdatedAt,
		&photoUser.User.Email,
		&photoUser.User.Username,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errs.NewNotFoundError("photo not found")
		}
		return nil, errs.NewInternalServerError("something went wrong")
	}

	result := photo_repository.PhotoUserMapped{}
	return result.HandleMappingPhotoWithUserByPhotoId(photoUser), nil
}

// UpdatePhoto implements photo_repository.PhotoRepository.
func (photoRepo *photoRepositoryImpl) UpdatePhoto(photoId int, photoPayload *entity.Photo) (*dto.PhotoUpdateResponse, errs.Error) {
	tx, err := photoRepo.db.Begin()

	if err != nil {
		tx.Rollback()
		return nil, errs.NewInternalServerError("something went wrong")
	}

	row := tx.QueryRow(UpdatePhotoQuery, photoId, photoPayload.Title, photoPayload.Caption, photoPayload.PhotoUrl)

	var photo dto.PhotoUpdateResponse

	err = row.Scan(
		&photo.Id,
		&photo.Title,
		&photo.Caption,
		&photo.PhotoUrl,
		&photo.UserId,
		&photo.UpdatedAt,
	)

	if err != nil {
		tx.Rollback()
		return nil, errs.NewInternalServerError("something went wrong")
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return nil, errs.NewInternalServerError("something went wrong")
	}

	return &photo, nil
}

// DeletePhoto implements photo_repository.PhotoRepository.
func (photoRepo *photoRepositoryImpl) DeletePhoto(photoId int) errs.Error {
	tx, err := photoRepo.db.Begin()

	if err != nil {
		tx.Rollback()
		return errs.NewInternalServerError("something went wrong")
	}

	_, err = tx.Exec(deletePhotoById, photoId)

	if err != nil {
		tx.Rollback()
		return errs.NewInternalServerError("something went wrong")
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return errs.NewInternalServerError("something went wrong")
	}

	return nil
}
