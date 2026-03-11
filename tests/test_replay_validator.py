from replay_validator import MAX_CONTENT_LENGTH, MIN_FILE_SIZE, secure_filename
from replay_validator import validate as validate_replay

VALID_SIZE = 300 * 1024  # 300KB — within bounds


class TestSecureFilename:
    def test_strips_path_traversal(self):
        assert secure_filename("../../../etc/match.replay") == "match.replay"

    def test_replaces_special_chars(self):
        assert secure_filename("my file!.replay") == "my_file_.replay"

    def test_strips_leading_dots(self):
        assert secure_filename("...hidden.replay") == "hidden.replay"

    def test_normal_filename_unchanged(self):
        assert secure_filename("match.replay") == "match.replay"


class TestValidate:
    def test_valid_file(self):
        safe_name, error, status_code = validate_replay("match.replay", VALID_SIZE)
        assert error is None
        assert status_code == 200
        assert safe_name == "match.replay"

    def test_wrong_extension(self):
        _, error, status_code = validate_replay("match.txt", VALID_SIZE)
        assert error is not None
        assert status_code == 400

    def test_empty_filename(self):
        _, error, status_code = validate_replay("", VALID_SIZE)
        assert error is not None
        assert status_code == 400

    def test_dotfile_edge_case(self):
        # "....replay" sanitizes to ".replay" (no stem) — must be rejected
        _, error, status_code = validate_replay("....replay", VALID_SIZE)
        assert error is not None
        assert status_code == 400

    def test_path_traversal_sanitized_to_valid(self):
        # ../match.replay → match.replay, which is valid
        safe_name, error, status_code = validate_replay(
            "../../../match.replay", VALID_SIZE
        )
        assert error is None
        assert ".." not in safe_name

    def test_too_large(self):
        oversized = MAX_CONTENT_LENGTH + 1
        _, error, status_code = validate_replay("match.replay", oversized)
        assert error is not None
        assert status_code == 413

    def test_too_small(self):
        _, error, status_code = validate_replay("match.replay", 100)
        assert error is not None
        assert status_code == 400

    def test_exact_min_size_accepted(self):
        _, error, _ = validate_replay("match.replay", MIN_FILE_SIZE)
        assert error is None

    def test_exact_max_size_accepted(self):
        _, error, _ = validate_replay("match.replay", MAX_CONTENT_LENGTH)
        assert error is None
