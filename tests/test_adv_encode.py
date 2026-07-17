"""Tests for adv_encode module — text encoding utilities."""

from scripts.adv_encode import (
    _encode,
    _normalize_adv_translation_punctuation,
    _processEMtag,
    START_EM_LENGTH,
    END_EM_LENGTH,
)


class TestEncodeConstants:
    def test_em_tag_lengths(self):
        assert START_EM_LENGTH == len("<em>")
        assert END_EM_LENGTH == len("</em>")


class TestEncodeEdgeCases:
    def test_all_special_chars_combined(self):
        result = _encode("a\nb\rc~d....e...f.g")
        assert result == "a\\nb\\rc～d……e…f.g"

    def test_five_dots(self):
        assert _encode(".....") == "……"

    def test_six_dots(self):
        assert _encode("......") == "……"

    def test_seven_dots_partial(self):
        # 7 dots: first 4-6 match → ……, remaining 1-3 match → …
        result = _encode(".......")
        assert "……" in result

    def test_only_tilde(self):
        assert _encode("~") == "～"

    def test_only_newlines(self):
        assert _encode("\n\r") == "\\n\\r"

    def test_exact_double_hyphen_is_adv_interruption_dash(self):
        assert _normalize_adv_translation_punctuation("잠깐--그건") == "잠깐――그건"

    def test_longer_hyphen_run_is_not_rewritten_as_dialogue_dash(self):
        assert _normalize_adv_translation_punctuation("https://example.invalid/a---b") == "https://example.invalid/a---b"

    def test_url_tag_and_placeholder_are_not_rewritten(self):
        source = 'https://example.invalid/a--b <em\\=a--b>{key--x} 그리고--멈춰'
        expected = 'https://example.invalid/a--b <em\\=a--b>{key--x} 그리고――멈춰'
        assert _normalize_adv_translation_punctuation(source) == expected


class TestProcessEMtagEdgeCases:
    def test_three_words(self):
        result = _processEMtag("<em>a b c</em>")
        assert result == "<em>a</em> <em>b</em> <em>c</em>"

    def test_nested_text_before_em(self):
        result = _processEMtag("prefix <em>word</em> suffix")
        assert result == "prefix <em>word</em> suffix"

    def test_text_with_em_at_end(self):
        result = _processEMtag("text <em>a b</em>")
        assert result == "text <em>a</em> <em>b</em>"

    def test_empty_em_tags(self):
        result = _processEMtag("<em></em>")
        assert result == "<em></em>"
