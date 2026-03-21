"""Korean language utilities for particle (조사) correction at array boundaries.

Pure Python implementation — no external dependencies.
Handles batchim (받침) detection and particle allomorph selection.
"""

import re

# Korean Unicode range: 가(0xAC00) ~ 힣(0xD7A3)
# Each syllable = (초성 * 21 + 중성) * 28 + 종성
# 종성 0 = no batchim
_HANGUL_START = 0xAC00
_HANGUL_END = 0xD7A3
_JONGSEONG_RIEUL = 8  # ㄹ is jongseong index 8


def has_batchim(char: str):
    """Check if a Korean character has batchim (final consonant).

    Returns True (has batchim), False (no batchim), or None (not Korean).
    """
    if not char or len(char) != 1:
        return None
    code = ord(char)
    if not (_HANGUL_START <= code <= _HANGUL_END):
        return None
    jongseong = (code - _HANGUL_START) % 28
    return jongseong != 0


def _get_jongseong(char: str):
    """Get jongseong index of a Korean character. None if not Korean."""
    if not char or len(char) != 1:
        return None
    code = ord(char)
    if not (_HANGUL_START <= code <= _HANGUL_END):
        return None
    return (code - _HANGUL_START) % 28


def last_korean_char(text: str):
    """Find the last Korean character in text, ignoring trailing spaces/HTML/punctuation.

    Returns the character or None if no Korean character found.
    """
    if not text:
        return None
    # Strip trailing whitespace and HTML tags
    cleaned = re.sub(r'[\s<][^>]*>?\s*$', '', text.rstrip())
    if not cleaned:
        cleaned = text.rstrip()
    # Walk backwards to find last Korean char
    for ch in reversed(cleaned):
        if _HANGUL_START <= ord(ch) <= _HANGUL_END:
            return ch
    return None


# Particle pairs: (no_batchim_form, batchim_form)
# For 로/으로: special case — ㄹ batchim uses 로
PARTICLE_PAIRS = {
    "가": ("가", "이"),
    "이": ("가", "이"),
    "를": ("를", "을"),
    "을": ("를", "을"),
    "는": ("는", "은"),
    "은": ("는", "은"),
    "와": ("와", "과"),
    "과": ("와", "과"),
    "로": ("로", "으로"),   # ㄹ batchim → 로 (special)
    "으로": ("로", "으로"),
}

# Dual-form notations: 이(가), 을(를), etc.
_DUAL_FORM_PATTERN = re.compile(r'^(이)\(가\)|^(가)\(이\)|^(을)\(를\)|^(를)\(을\)|^(은)\(는\)|^(는)\(은\)|^(와)\(과\)|^(과)\(와\)')

# Words that START with particle-like characters but are NOT particles
_FALSE_PARTICLE_WORDS = re.compile(
    r'^(?:이하|이상|이후|이전|이번|이것|이런|이때|가능|가장|가끔|의욕|의미|로그|로딩|은근|은밀)'
)

# Pattern to detect particle at start of text
# Particle must be followed by space, end of string, or non-Korean character
_PARTICLE_DETECT = re.compile(
    r'^(으로|이|가|를|을|은|는|와|과|로|에|의|도|만|까지|부터|에서|만큼)(?:\s|$|[^가-힣])'
)


def is_particle_start(text: str):
    """Detect if text starts with a Korean particle.

    Returns the particle string if found, None otherwise.
    Excludes false positives like 이하, 이상, 가능, 의욕, etc.
    """
    if not text or not text.strip():
        return None
    stripped = text.strip()

    # Check for false positives first
    if _FALSE_PARTICLE_WORDS.match(stripped):
        return None

    # Try to match particle
    m = _PARTICLE_DETECT.match(stripped)
    if m:
        return m.group(1)

    # Check bare particle (single char at end of string)
    if len(stripped) <= 2 and stripped in PARTICLE_PAIRS:
        return stripped

    return None


def correct_particle(prev_char: str, particle: str) -> str:
    """Select correct particle allomorph based on preceding character's batchim.

    Args:
        prev_char: The last Korean character of the preceding text.
        particle: The particle to correct.

    Returns:
        The corrected particle (may be unchanged if no correction needed).
    """
    if particle not in PARTICLE_PAIRS:
        return particle

    batchim = has_batchim(prev_char)
    if batchim is None:
        return particle  # Non-Korean: can't determine, return as-is

    no_batchim_form, batchim_form = PARTICLE_PAIRS[particle]

    # Special case: ㄹ batchim + 로/으로 → always 로
    if particle in ("로", "으로"):
        jongseong = _get_jongseong(prev_char)
        if jongseong == _JONGSEONG_RIEUL:
            return "로"

    if batchim:
        return batchim_form
    else:
        return no_batchim_form


def adjust_boundary(prev_text: str, next_text: str):
    """Adjust particle and spacing at the boundary between two array elements.

    Args:
        prev_text: Translation text of the preceding array element.
        next_text: Translation text of the current array element.

    Returns:
        (adjusted_prev, adjusted_next) tuple.
    """
    if not prev_text or not next_text:
        return prev_text, next_text

    adjusted_prev = prev_text
    adjusted_next = next_text

    # Step 1: Handle dual-form notation like 이(가), 을(를)
    dual_match = _DUAL_FORM_PATTERN.match(adjusted_next)
    if dual_match:
        # Extract the base particle from whichever group matched
        base_particle = next(g for g in dual_match.groups() if g is not None)
        remainder = adjusted_next[dual_match.end():]
        prev_char = last_korean_char(adjusted_prev)
        if prev_char:
            corrected = correct_particle(prev_char, base_particle)
            adjusted_next = corrected + remainder
            # Also strip trailing space from prev
            adjusted_prev = adjusted_prev.rstrip()
            return adjusted_prev, adjusted_next

    # Step 2: Detect particle at start of next_text
    particle = is_particle_start(adjusted_next)
    if particle is None:
        return adjusted_prev, adjusted_next

    # Step 3: Get last Korean char from prev_text
    prev_char = last_korean_char(adjusted_prev)
    if prev_char is None:
        return adjusted_prev, adjusted_next

    # Step 4: Strip trailing space from prev_text before particle
    adjusted_prev = adjusted_prev.rstrip()

    # Step 5: Correct particle allomorph
    corrected = correct_particle(prev_char, particle)
    if corrected != particle:
        adjusted_next = corrected + adjusted_next[len(particle):]

    return adjusted_prev, adjusted_next
