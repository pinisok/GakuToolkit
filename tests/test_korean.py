"""Tests for Korean language utilities (particle correction, batchim detection)."""

import pytest

from scripts.korean import (
    has_batchim,
    last_korean_char,
    correct_particle,
    adjust_boundary,
    is_particle_start,
    PARTICLE_PAIRS,
)


# ============================================================
# has_batchim: detect final consonant (받침) in Korean syllable
# ============================================================


class TestHasBatchim:
    def test_vowel_ending(self):
        """Characters ending in vowel have no batchim."""
        assert has_batchim("가") is False
        assert has_batchim("기") is False
        assert has_batchim("카") is False
        assert has_batchim("터") is False
        assert has_batchim("드") is False
        assert has_batchim("호") is False

    def test_consonant_ending(self):
        """Characters with batchim return True."""
        assert has_batchim("중") is True
        assert has_batchim("욕") is True
        assert has_batchim("듣") is True
        assert has_batchim("한") is True
        assert has_batchim("길") is True
        assert has_batchim("집") is True

    def test_rieul_batchim(self):
        """ㄹ batchim is special for 로/으로 selection."""
        assert has_batchim("길") is True
        assert has_batchim("설") is True
        assert has_batchim("발") is True

    def test_non_korean(self):
        """Non-Korean characters return None."""
        assert has_batchim("a") is None
        assert has_batchim("1") is None
        assert has_batchim(">") is None
        assert has_batchim(" ") is None

    def test_empty_string(self):
        """Empty string returns None."""
        assert has_batchim("") is None


# ============================================================
# last_korean_char: extract last Korean character from string
# ============================================================


class TestLastKoreanChar:
    def test_simple_word(self):
        assert last_korean_char("원기") == "기"
        assert last_korean_char("집중") == "중"
        assert last_korean_char("호조") == "조"

    def test_trailing_space(self):
        """Should ignore trailing spaces."""
        assert last_korean_char("원기 ") == "기"
        assert last_korean_char("집중  ") == "중"

    def test_trailing_html(self):
        """Should ignore trailing HTML tags."""
        assert last_korean_char("호조</nobr>") == "조"
        assert last_korean_char("원기 </nobr>") == "기"

    def test_number_only(self):
        """Pure numbers should return None."""
        assert last_korean_char("100%") is None
        assert last_korean_char("1.3") is None

    def test_mixed(self):
        """Mixed content should find last Korean char."""
        assert last_korean_char("레슨 CLEAR") == "슨"
        assert last_korean_char("100파라미터") == "터"

    def test_empty(self):
        assert last_korean_char("") is None
        assert last_korean_char("   ") is None


# ============================================================
# correct_particle: select right allomorph based on batchim
# ============================================================


class TestCorrectParticle:
    def test_ga_i_vowel(self):
        """Vowel ending → 가."""
        assert correct_particle("기", "이") == "가"
        assert correct_particle("기", "가") == "가"

    def test_ga_i_consonant(self):
        """Consonant ending → 이."""
        assert correct_particle("중", "가") == "이"
        assert correct_particle("중", "이") == "이"

    def test_reul_eul_vowel(self):
        """Vowel ending → 를."""
        assert correct_particle("기", "을") == "를"
        assert correct_particle("기", "를") == "를"

    def test_reul_eul_consonant(self):
        """Consonant ending → 을."""
        assert correct_particle("중", "를") == "을"
        assert correct_particle("중", "을") == "을"

    def test_neun_eun_vowel(self):
        """Vowel ending → 는."""
        assert correct_particle("기", "은") == "는"

    def test_neun_eun_consonant(self):
        """Consonant ending → 은."""
        assert correct_particle("중", "는") == "은"

    def test_wa_gwa_vowel(self):
        """Vowel ending → 와."""
        assert correct_particle("기", "과") == "와"

    def test_wa_gwa_consonant(self):
        """Consonant ending → 과."""
        assert correct_particle("중", "와") == "과"

    def test_ro_euro_vowel(self):
        """Vowel ending → 로."""
        assert correct_particle("기", "으로") == "로"
        assert correct_particle("기", "로") == "로"

    def test_ro_euro_consonant(self):
        """Consonant ending → 으로 (except ㄹ)."""
        assert correct_particle("중", "로") == "으로"

    def test_ro_euro_rieul(self):
        """ㄹ batchim → 로 (not 으로)."""
        assert correct_particle("길", "으로") == "로"
        assert correct_particle("길", "로") == "로"

    def test_non_paired_particle(self):
        """Particles without pairs (의, 에, 도, 만) return unchanged."""
        assert correct_particle("기", "의") == "의"
        assert correct_particle("중", "의") == "의"
        assert correct_particle("기", "에") == "에"

    def test_non_korean_prev(self):
        """Non-Korean preceding text returns particle unchanged."""
        assert correct_particle("5", "가") == "가"
        assert correct_particle(">", "이") == "이"


# ============================================================
# is_particle_start: detect if text starts with a particle
# ============================================================


class TestIsParticleStart:
    def test_particle_starts(self):
        assert is_particle_start("가 0일 때") == "가"
        assert is_particle_start("이 3 이상") == "이"
        assert is_particle_start("를 소비") == "를"
        assert is_particle_start("을 제외") == "을"
        assert is_particle_start("의 효과") == "의"
        assert is_particle_start("로 변경") == "로"
        assert is_particle_start("으로 변경") == "으로"
        assert is_particle_start("에 들어간다") == "에"

    def test_non_particle_starts(self):
        """Words starting with particle-like chars but aren't particles."""
        assert is_particle_start("이하일 때") is None
        assert is_particle_start("이상일 때") is None
        assert is_particle_start("이후") is None
        assert is_particle_start("가능한") is None
        assert is_particle_start("의욕") is None
        assert is_particle_start("로그") is None

    def test_bare_particle(self):
        """Bare particle (just the particle, no following text)."""
        assert is_particle_start("가") == "가"
        assert is_particle_start("을") == "을"

    def test_empty(self):
        assert is_particle_start("") is None
        assert is_particle_start("   ") is None

    def test_non_korean_start(self):
        assert is_particle_start("100%") is None
        assert is_particle_start("<nobr>") is None


# ============================================================
# adjust_boundary: full boundary adjustment
# ============================================================


class TestAdjustBoundary:
    def test_strip_trailing_space_before_particle(self):
        """Remove trailing space from prev when next starts with particle."""
        prev, next_text = adjust_boundary("원기 ", "가 0일 때")
        assert prev == "원기"
        assert next_text == "가 0일 때"

    def test_correct_particle_vowel(self):
        """Correct particle for vowel-ending word."""
        prev, next_text = adjust_boundary("원기", "이 0일 때")
        assert next_text == "가 0일 때"

    def test_correct_particle_consonant(self):
        """Correct particle for consonant-ending word."""
        prev, next_text = adjust_boundary("집중", "가 3 이상")
        assert next_text == "이 3 이상"

    def test_combined_space_and_particle(self):
        """Both strip space and correct particle."""
        prev, next_text = adjust_boundary("집중 ", "가 3턴")
        assert prev == "집중"
        assert next_text == "이 3턴"

    def test_euro_to_ro(self):
        """으로 → 로 for vowel-ending."""
        prev, next_text = adjust_boundary("강기", "으로 변경")
        assert next_text == "로 변경"

    def test_ro_to_euro(self):
        """로 → 으로 for consonant-ending (not ㄹ)."""
        prev, next_text = adjust_boundary("집중", "로 변경")
        assert next_text == "으로 변경"

    def test_ro_stays_for_rieul(self):
        """로 stays 로 for ㄹ batchim."""
        prev, next_text = adjust_boundary("스킬", "로 변경")
        assert next_text == "로 변경"

    def test_no_particle_no_change(self):
        """Non-particle text unchanged."""
        prev, next_text = adjust_boundary("원기", "소비한다")
        assert prev == "원기"
        assert next_text == "소비한다"

    def test_non_korean_prev_no_change(self):
        """Number preceding: no particle correction."""
        prev, next_text = adjust_boundary("100%", "이하일 때")
        assert prev == "100%"
        assert next_text == "이하일 때"

    def test_preserve_non_particle_content(self):
        """의욕, 이상 etc. are words not particles."""
        prev, next_text = adjust_boundary("호조", "이상일 때")
        assert next_text == "이상일 때"

    def test_dual_form_notation(self):
        """이(가) notation: pick correct form."""
        prev, next_text = adjust_boundary("원기", "이(가) 0일 때")
        assert next_text == "가 0일 때"

    def test_dual_form_consonant(self):
        prev, next_text = adjust_boundary("집중", "이(가) 0일 때")
        assert next_text == "이 0일 때"

    def test_dual_form_eul_reul(self):
        prev, next_text = adjust_boundary("원기", "을(를) 소비")
        assert next_text == "를 소비"
