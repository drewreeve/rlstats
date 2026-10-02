import pytest

from server import MAX_UPLOAD_BYTES, secure_filename, validate_upload

VALID_SIZE = 1024  # 1KB


class TestSecureFilename:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            pytest.param(
                "../../../etc/match.replay", "match.replay", id="path-traversal"
            ),
            pytest.param("my file!.replay", "my_file_.replay", id="special-chars"),
            pytest.param("...hidden.replay", "hidden.replay", id="leading-dots"),
            pytest.param("match.replay", "match.replay", id="normal-unchanged"),
        ],
    )
    def test_sanitizes(self, raw: str, expected: str):
        assert secure_filename(raw) == expected


class TestValidateUpload:
    def test_valid_file(self):
        safe_name, error, status_code = validate_upload("match.replay", VALID_SIZE)
        assert error is None
        assert status_code == 200
        assert safe_name == "match.replay"

    @pytest.mark.parametrize(
        "filename",
        [
            pytest.param("match.txt", id="wrong-extension"),
            pytest.param("", id="empty"),
            # "....replay" sanitizes to ".replay" (no stem) — must be rejected
            pytest.param("....replay", id="dotfile-no-stem"),
        ],
    )
    def test_rejects_bad_filename(self, filename: str):
        _, error, status_code = validate_upload(filename, VALID_SIZE)
        assert error is not None
        assert status_code == 400

    def test_path_traversal_sanitized_to_valid(self):
        # ../match.replay → match.replay, which is valid
        safe_name, error, _ = validate_upload("../../../match.replay", VALID_SIZE)
        assert error is None
        assert ".." not in safe_name

    def test_too_large(self):
        oversized = MAX_UPLOAD_BYTES + 1
        _, error, status_code = validate_upload("match.replay", oversized)
        assert error is not None
        assert status_code == 413

    def test_exact_max_size_accepted(self):
        _, error, _ = validate_upload("match.replay", MAX_UPLOAD_BYTES)
        assert error is None
