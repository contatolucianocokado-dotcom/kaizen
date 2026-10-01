#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, re, sys
from pathlib import Path
import yaml
from google.ads.googleads.client import GoogleAdsClient

ROOT = Path(__file__).resolve().parents[1]
ACCOUNT_TEMPLATE = "{lpurl}?utm_source=googleads&utm_medium={network}&utm_campaign={campaignid}&utm_term={campaignid}_{keyword}&utm_content={campaignid}_{placement}&utm_creative={campaignid}_{creative}"
DEMAND_GEN_ADGROUP_TEMPLATE = "{lpurl}?utm_source=googleads&utm_medium={network}&utm_campaign={adgroupid}&utm_term={adgroupid}_{keyword}&utm_content={adgroupid}_{placement}&utm_creative={adgroupid}_{creative}"
BAD_TOKENS = ("{campaignId}", "{adgroupId}", "{creativeId}")

def clean_id(v): return re.sub(r"\D+", "", str(v or ""))

def load_client(cfg):
    ads = cfg["google_ads"]
    data = {
        "developer_token": ads["developer_token"],
        "client_id": ads["client_id"],
        "client_secret": ads["client_secret"],
        "refresh_token": ads["refresh_token"],
        "use_proto_plus": True,
    }
    login_customer_id = clean_id(ads.get("login_customer_id") or ads.get("customer_id"))
    if len(login_customer_id) == 10:
        data["login_customer_id"] = login_customer_id
    return GoogleAdsClient.load_from_dict(data, version=ads.get("api_version", "v23"))

def customers(cfg):
    out=[]
    for item in (cfg.get("google_ads") or {}).get("customers") or []:
        cid=clean_id(item.get("customer_id"))
        if cid: out.append({"customer_id":cid,"name":str(item.get("name") or ""),"site":str(item.get("site") or "")})
    return out

def update_customer_template(client, cid):
    service = client.get_service("CustomerService")
    op = client.get_type("CustomerOperation")
    op.update.resource_name = f"customers/{cid}"
    op.update.tracking_url_template = ACCOUNT_TEMPLATE
    op.update_mask.paths.append("tracking_url_template")
    return service.mutate_customer(customer_id=cid, operation=op).result.resource_name

def update_adgroup_template(client, cid, resource_name, template):
    service = client.get_service("AdGroupService")
    op = client.get_type("AdGroupOperation")
    op.update.resource_name = resource_name
    op.update.tracking_url_template = template
    op.update_mask.paths.append("tracking_url_template")
    return service.mutate_ad_groups(customer_id=cid, operations=[op]).results[0].resource_name

def clear_adgroup_template(client, cid, resource_name):
    service = client.get_service("AdGroupService")
    op = client.get_type("AdGroupOperation")
    op.update.resource_name = resource_name
    op.update_mask.paths.append("tracking_url_template")
    return service.mutate_ad_groups(customer_id=cid, operations=[op]).results[0].resource_name

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--config", default=str(ROOT / "config.yaml"))
    ap.add_argument("--fix", action="store_true")
    args=ap.parse_args()
    cfg=yaml.safe_load(Path(args.config).read_text()) or {}
    try:
        client=load_client(cfg)
    except Exception as e:
        print(json.dumps({"ok": False, "accounts": 0, "accounts_with_issues_or_errors": 1, "error": str(e)}, ensure_ascii=False))
        return
    gas=client.get_service("GoogleAdsService")
    summary=[]
    for account in customers(cfg):
        cid=account["customer_id"]
        rec={"customer_id":cid,"name":account["name"],"site":account["site"],"account_fixed":False,"adgroups_fixed":0,"non_dg_cleared":0,"errors":[]}
        try:
            q="SELECT customer.id, customer.descriptive_name, customer.tracking_url_template FROM customer"
            current=""
            for batch in gas.search_stream(customer_id=cid, query=q):
                for row in batch.results:
                    current=str(row.customer.tracking_url_template or "")
            rec["account_template"] = current
            rec["account_ok"] = current == ACCOUNT_TEMPLATE
            if args.fix and current != ACCOUNT_TEMPLATE:
                update_customer_template(client, cid)
                rec["account_fixed"] = True
        except Exception as e:
            rec["errors"].append("customer_template: " + str(e).split("\n")[0][:250])
        try:
            q="""
            SELECT campaign.id, campaign.name, campaign.advertising_channel_type, campaign.status,
                   ad_group.id, ad_group.name, ad_group.status, ad_group.resource_name, ad_group.tracking_url_template
            FROM ad_group
            WHERE campaign.status != REMOVED AND ad_group.status != REMOVED
            """
            for batch in gas.search_stream(customer_id=cid, query=q):
                for row in batch.results:
                    channel=str(row.campaign.advertising_channel_type.name if hasattr(row.campaign.advertising_channel_type,'name') else row.campaign.advertising_channel_type)
                    tpl=str(row.ad_group.tracking_url_template or "")
                    resource=str(row.ad_group.resource_name)
                    if channel == "DEMAND_GEN":
                        if tpl != DEMAND_GEN_ADGROUP_TEMPLATE:
                            rec.setdefault("bad_demand_gen", []).append({"campaign_id":str(row.campaign.id),"ad_group_id":str(row.ad_group.id),"template":tpl})
                            if args.fix:
                                update_adgroup_template(client, cid, resource, DEMAND_GEN_ADGROUP_TEMPLATE)
                                rec["adgroups_fixed"] += 1
                    else:
                        if tpl and ("utm_campaign" in tpl or any(t in tpl for t in BAD_TOKENS) or "{adgroupid}" in tpl):
                            rec.setdefault("bad_non_dg", []).append({"campaign_id":str(row.campaign.id),"ad_group_id":str(row.ad_group.id),"channel":channel,"template":tpl})
                            if args.fix:
                                clear_adgroup_template(client, cid, resource)
                                rec["non_dg_cleared"] += 1
        except Exception as e:
            rec["errors"].append("adgroup_templates: " + str(e).split("\n")[0][:250])
        summary.append(rec)
        print(json.dumps(rec, ensure_ascii=False))
    bad=sum(0 if r.get('account_ok') and not r.get('bad_demand_gen') and not r.get('bad_non_dg') and not r.get('errors') else 1 for r in summary)
    print(json.dumps({"ok": bad == 0, "accounts": len(summary), "accounts_with_issues_or_errors": bad}, ensure_ascii=False))

if __name__ == "__main__":
    main()
