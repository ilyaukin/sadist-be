# Project-Specific Gotchas and Errors

- **Image Uploads:**
  - `IMAGE_CONFIG` in `app/image.py` must be updated for each new image type. Currently, only `avatar` is supported.
  - Image size and dimensions are strictly validated.
- **User Signup:**
  - Registration is a two-step process. Data is stored in `toConfirm` until the user clicks the link in the email.
  - `BASE_URL` environment variable should be set for the confirmation links to be generated correctly in all environments.
- **Authentication:**
  - Passwords are MD5 hashed with a salt.
- **Database:**
  - Uses `mongomoron` for DB operations, which might have different behavior than standard PyMongo in some cases.
