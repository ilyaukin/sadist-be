# Project Tasks

## Roadmap / Pending Tasks
- [ ] Implement more image types in `IMAGE_CONFIG`.
- [ ] Add more OAuth providers (currently only Google is implemented).
- [ ] Improve password hashing (currently using MD5).

## Completed Tasks

### Image Uploading
- **Endpoints:**
  - `POST /image/<image_type>`: Upload an image. Expects file in `file` or `<image_type>` field.
  - `GET /image/<image_type>/<filename>`: Retrieve an uploaded image.
- **Use cases:**
  - Avatars
  - Images in data sheets (not implemented yet)
- **Logic:**
  - Validates image properties (size, dimensions) based on `IMAGE_CONFIG` in `app/image.py`.
  - Checks permissions based on `IMAGE_CONFIG`.
  - Saves files to MongoDB GridFS with a unique ID appended to the filename.
- **Files:** `app/image_api.py`, `app/image.py`

### User Signup
- **Endpoints:**
  - `POST /user/signup`: Initiates user registration.
  - `GET /user/confirm/<confirmation_hash>`: Confirms registration via email link.
- **Logic:**
  - Implements a two-step registration process.
  - `signup` adds data to `toConfirm` field in the database and sends a confirmation email.
  - `confirm` moves data from `toConfirm` to the main user record and removes the confirmation hash.
  - Uses MD5 hashing with a salt for local passwords.
  - Prevents hijacking existing logins but allows restarting registration if email is not confirmed.
- **Files:** `app/user.py`
