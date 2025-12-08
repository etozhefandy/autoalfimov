import json
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# твой токен и рекламный аккаунт
FB_ACCESS_TOKEN = "EAASZCrBwhoH0BO6hvTPZBtAX3OFPcJjZARZBZCIllnjc4GkxagyhvvrylPKWdU9jMijZA051BJRRvVuV1nab4k5jtVO5q0TsDIKbXzphumaFIbqKDcJ3JMvQTmORdrNezQPZBP14pq4NKB56wpIiNJSLFa5yXFsDttiZBgUHAmVAJknN7Ig1ZBVU2q0vRyQKJtyuXXwZDZD"
AD_ACCOUNT_ID = "act_1437151187825723"  # или любой другой act_..., который хочешь проверить


def main():
    # НИКАКИХ app_id и app_secret, только токен
    FacebookAdsApi.init(access_token=FB_ACCESS_TOKEN)

    acc = AdAccount(AD_ACCOUNT_ID)

    fields = [
        "id",
        "name",
        "effective_status",
        "created_time",
        "creative{instagram_permalink_url,effective_object_story_id,effective_instagram_media_id,object_story_spec,object_story_id}",
    ]

    params = {
        "effective_status": ["ACTIVE"],
    }

    ads = acc.get_ads(fields=fields, params=params)

    out = []
    for ad in ads:
        out.append(ad.export_all_data())

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()