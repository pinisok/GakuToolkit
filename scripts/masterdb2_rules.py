"""
MasterDB2 rule definitions.

Maps data type names to their field translation rules (JP field name → KR column header).
Extracted from masterdb2.py to reduce file size.
"""

rule_key_translate_map = {
    "Achievement": {
        "id": "id",
        "name": "이름",
        "description": "설명"
    },
    "Character": {
        "id": "id",
        "lastName": "이름",
        "firstName": "성"
    },
    "CharacterAdv": {
        "characterId": "캐릭터 ID",
        "name": "이름",
        "regexp": "regexp"
    },
    "CharacterDearnessLevel": {
        "characterId": "캐릭터 ID",
        "dearnessLevel": "친애도 레벨",
        "produceConditionDescription": "해금 조건 설명"
    },
    "CharacterDetail": {
        "characterId": "캐릭터 ID",
        "type": "종류",
        "order": "",
        "content": ""
    },
    "CharacterPushMessage": {
        "characterId": "캐릭터 ID",
        "type": "종류",
        "number": "",
        "title": "제목",
        "message": ""
    },
    "CoinGashaButton": {
        "id": "id",
        "name": "이름",
        "description": "설명"
    },
    "Costume": {
        "id": "id",
        "name": "이름",
        "description": "설명"
    },
    "CostumeHead": {
        "id": "id",
        "name": "이름",
        "description": "설명"
    },
    "EffectGroup": {
        "id": "id",
        "name": "이름"
    },
    "EventLabel": {
        "eventType": "",
        "name": "이름"
    },
    "FeatureLock": {
        "tutorialType": "",
        "name": "이름",
        "description": "설명",
        "routeDescription": ""
    },
    "GashaButton": {
        "id": "id",
        "order": "",
        "name": "이름",
        "description": "설명"
    },
    "GvgRaid": {
        "id": "id",
        "order": "",
        "name": "이름"
    },
    "HelpCategory": {
        "id": "id",
        "order": "",
        "name": "이름",
        "texts": ""
    },
    "HelpContent": {
        "helpCategoryId": "",
        "id": "id",
        "order": "",
        "name": "이름"
    },
    "IdolCard": {
        "id": "id",
        "name": "이름"
    },
    "Item": {
        "id": "id",
        "name": "이름",
        "description": "설명",
        "acquisitionRouteDescription": ""
    },
    "Localization": {
        "id": "id",
        "description": "설명"
    },
    "MainStoryChapter": {
        "mainStoryPartId": "",
        "id": "id",
        "title": "제목",
        "description": "설명"
    },
    "MainStoryPart": {
        "id": "id",
        "title": "제목"
    },
    "MainTask": {
        "mainTaskGroupId": "",
        "number": "",
        "title": "제목",
        "description": "설명",
        "homeDescription": ""
    },
    "MainTaskGroup": {
        "id": "id",
        "title": "제목"
    },
    "Media": {
        "id": "id",
        "name": "이름"
    },
    "MeishiBaseAsset": {
        "id": "id",
        "name": "이름"
    },
    "MeishiIllustrationAsset": {
        "id": "id",
        "name": "이름"
    },
    "MemoryGift": {
        "id": "id",
        "name": "이름"
    },
    "MemoryTag": {
        "id": "id",
        "defaultName": ""
    },
    "Mission": {
        "id": "id",
        "name": "이름"
    },
    "MissionGroup": {
        "id": "id",
        "name": "이름"
    },
    "MissionPanelSheet": {
        "missionPanelSheetGroupId": "",
        "number": "",
        "name": "이름"
    },
    "MissionPanelSheetGroup": {
        "id": "id",
        "name": "이름"
    },
    "MissionPass": {
        "id": "id",
        "name": "이름",
        "description": "설명"
    },
    "MissionPassPoint": {
        "idname": ""
    },
    "MissionPoint": {
        "id": "id",
        "name": "이름"
    },
    "Music": {
        "id": "id",
        "title": "제목",
        "displayTitle": "표시되는 제목",
        "lyrics": "작사",
        "composer": "작곡",
        "arranger": "편곡"
    },
    "PhotoBackground": {
        "id": "id",
        "name": "이름"
    },
    "PhotoFacialMotionGroup": {
        "id": "id",
        "number": "숫자",
        "name": "이름"
    },
    "PhotoPose": {
        "id": "id",
        "name": "이름"
    },
    "Produce": {
        "id": "id",
        "name": "이름"
    },
    "ProduceAdv": {
        "produceType": "프로듀스 종류",
        "type": "종류",
        "title": "제목"
    },
    "ProduceCard": {
        "id": "id",
        "upgradeCount": "",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": "",
        "name": "이름",
        "text": "텍스트"
    },
    "ProduceCardSearch": {
        "id": "id",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": "",
        "text": "텍스트"
    },
    "ProduceCardStatusEnchant": {
        "id": "id",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": "",
        "text": "텍스트"
    },
    "ProduceCardTag": {
        "id": "id",
        "name": "이름"
    },
    "ProduceChallengeSlot": {
        "id": "id",
        "number": "",
        "unlockDescription": ""
    },
    "ProduceCharacterAdv": {
        "assetId": "",
        "title": "제목"
    },
    "ProduceDescription": {
        "id": "id",
        "name": "이름",
        "swapName": ""
    },
    "ProduceDescriptionExamEffect": {
        "type": "종류",
        "name": "이름"
    },
    "ProduceDescriptionLabel": {
        "id": "id",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": "",
        "name": "이름",
        "text": "텍스트"
    },
    "ProduceDescriptionProduceCardGrowEffect": {
        "type": "종류",
        "name": "이름"
    },
    "ProduceDescriptionProduceCardGrowEffectType": {
        "type": "종류",
        "name": "이름",
        "produceCardCustomizeTemplate": ""
    },
    "ProduceDescriptionProduceEffect": {
        "type": "종류",
        "name": "이름"
    },
    "ProduceDescriptionProduceEffectType": {
        "type": "종류",
        "name": "이름"
    },
    "ProduceDescriptionProduceExamEffectType": {
        "type": "종류",
        "name": "이름",
        "swapName": ""
    },
    "ProduceDescriptionProducePlan": {
        "type": "종류",
        "name": "이름"
    },
    "ProduceDescriptionProducePlanType": {
        "type": "종류",
        "name": "이름"
    },
    "ProduceDescriptionProduceStep": {
        "type": "종류",
        "name": "이름"
    },
    "ProduceDescriptionSwap": {
        "id": "id",
        "swapType": "",
        "text": "텍스트"
    },
    "ProduceDrink": {
        "id": "id",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": "",
        "name": "이름",
        "text": "텍스트"
    },
    "ProduceEventCharacterGrowth": {
        "characterId": "캐릭터 ID",
        "number": "",
        "title": "제목",
        "description": "설명"
    },
    "ProduceExamBattleNpcMob": {
        "id": "id",
        "name": "이름"
    },
    "ProduceExamEffect": {
        "id": "id",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": "",
        "text": "텍스트"
    },
    "ProduceExamGimmickEffectGroup": {
        "id": "id",
        "priority": "",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": "",
        "text": "텍스트"
    },
    "ProduceExamStatusEnchant": {
        "id": "id",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": "",
        "text": "텍스트"
    },
    "ProduceExamTrigger": {
        "id": "id",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": "",
        "text": "텍스트"
    },
    "ProduceGroup": {
        "id": "id",
        "name": "이름",
        "description": "설명"
    },
    "ProduceHighScore": {
        "id": "id",
        "name": "이름"
    },
    "ProduceItem": {
        "id": "id",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": "",
        "name": "이름",
        "text": "텍스트"
    },
    "ProduceNavigation": {
        "id": "id",
        "number": "",
        "description": "설명"
    },
    "ProduceSkill": {
        "id": "id",
        "level": "",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": "",
        "text": "텍스트"
    },
    "ProduceStepEventDetail": {
        "id": "id",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": ""
    },
    "ProduceStepEventSuggestion": {
        "id": "id",
        "produceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "targetId": ""
    },
    "ProduceStepLesson": {
        "id": "id",
        "name": "이름"
    },
    "ProduceStory": {
        "id": "id",
        "title": "제목",
        "produceEventHintProduceConditionDescriptions": ""
    },
    "PvpRateConfig": {
        "id": "id",
        "description": "설명"
    },
    "Rule": {
        "type": "종류",
        "platformType": "",
        "number": "",
        "html": ""
    },
    "SeminarExamTransition": {
        "examEffectType": "",
        "isLessonInt": "",
        "seminarExamId": "",
        "description": "설명",
        "seminarExamGroupName": "",
        "seminarExamName": ""
    },
    "Setting": {
        "id": "id",
        "initialUserName": "",
        "banWarningMessage": ""
    },
    "Shop": {
        "id": "id",
        "name": "이름"
    },
    "ShopItem": {
        "id": "id",
        "name": "이름"
    },
    "Story": {
        "id": "id",
        "title": "제목"
    },
    "StoryEvent": {
        "id": "id",
        "title": "제목"
    },
    "StoryGroup": {
        "id": "id",
        "title": "제목"
    },
    "SupportCard": {
        "id": "id",
        "upgradeProduceCardProduceDescriptions": "",
        "produceDescriptionType": "",
        "examDescriptionType": "",
        "examEffectType": "",
        "produceCardGrowEffectType": "",
        "produceCardCategory": "",
        "produceCardMovePositionType": "",
        "produceStepType": "",
        "name": "이름",
        "text": "텍스트"
    },
    "SupportCardFlavor": {
        "supportCardId": "",
        "number": "",
        "text": "텍스트"
    },
    "Terms": {
        "type": "종류",
        "name": "이름"
    },
    "Tips": {
        "id": "id",
        "title": "제목",
        "description": "설명"
    },
    "Tower": {
        "id": "id",
        "title": "제목"
    },
    "Tutorial": {
        "tutorialType": "",
        "step": "",
        "subStep": "",
        "texts": ""
    },
    "TutorialProduceStep": {
        "stepNumber": "",
        "tutorialStep": "",
        "stepType": "",
        "name": "이름"
    },
    "VoiceGroup": {
        "id": "id",
        "voiceAssetId": "",
        "title": "제목"
    },
    "VoiceRoster": {
        "characterId": "캐릭터 ID",
        "assetId": "",
        "title": "제목"
    },
    "Work": {
        "type": "종류",
        "name": "이름"
    }
}
rule_key_reverse_translate_map = {}
for name, rules in enumerate(rule_key_translate_map):
    rule_key_reverse_translate_map[name] = {}
    for key, value in enumerate(rule_key_translate_map):
        if value == "":
            continue
        rule_key_reverse_translate_map[name][value] = key

def TranslateRuleKey(file_name:str, target_key:str):
    splitted_keys = target_key.split(".")
    translated_keys:list = []
    for key in splitted_keys:
        translated_value = rule_key_translate_map[file_name].get(key, key)
        if translated_value == "":
            translated_value = key
        translated_keys.append(translated_value)
    return ".".join(translated_keys)

def TranslateReverseRuleKey(file_name:str, target_key:str):
    splitted_keys = target_key.split(".")
    translated_keys:list = []
    for key in splitted_keys:
        translated_keys.append(rule_key_translate_map[file_name].get(key, key))
    return ".".join(translated_keys)
