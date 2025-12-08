import json
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

FB_ACCESS_TOKEN = "EAASZCrBwhoH0BO6hvTPZBtAX3OFPcJjZARZBZCIllnjc4GkxagyhvvrylPKWdU9jMijZA051BJRRvVuV1nab4k5jtVO5q0TsDIKbXzphumaFIbqKDcJ3JMvQTmORdrNezQPZBP14pq4NKB56wpIiNJSLFa5yXFsDttiZBgUHAmVAJknN7Ig1ZBVU2q0vRyQKJtyuXXwZDZD"
AD_ACCOUNT_ID = "act_1437151187825723"  # дримкэмп


def main():
    # Отключаем строгую проверку appsecret_proof
    FacebookAdsApi.set_appsecret_proof_enabled(False)

    FacebookAdsApi.init(access_token=FB_ACCESS_TOKEN)

    acc = AdAccount(AD_ACCOUNT_ID)

    fields = [
        "id",
        "name",
        "effective_status",
        "created_time",
        "creative{instagram_permalink_url,effective_object_story_id,effective_instagram_media_id,object_story_spec,object_story_id}",
    ]

    ads = acc.get_ads(fields=fields)

    out = [ad.export_all_data() for ad in ads]
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()