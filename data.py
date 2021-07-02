# import boto3
# import os
import numpy as np
from google.ads.googleads.client import GoogleAdsClient
from campaignControl import campaign
from pprint import pprint

import pandas as pd
from dotenv import dotenv_values

# config = dotenv_values(".env")
# os.environ["aws_access_key_id"] = config["aws_access_key_id"]
# os.environ["aws_secret_access_key"] = config["aws_secret_access_key"]
# os.environ["region_name"] = config["region_name"]


# [START get_keyword_stats]

def maincode(client, customer_id, camp, campnew,
             start_date = "'2021-01-01'", end_date = "'2021-06-30'"):



    categoryDf = pd.read_csv("category.csv")
    dataList = []
    campaignNameDataDataFrame = []
    uniqueCampaignList = []
    ga_service = client.get_service("GoogleAdsService")

    query = """
    SELECT
        metrics.all_conversions,
        metrics.average_cpc,
        metrics.bounce_rate,
        metrics.clicks,
        keyword_view.resource_name,
        metrics.ctr,
        metrics.cost_micros,
        metrics.impressions,
        metrics.search_impression_share,
        metrics.search_click_share,
        ad_group_criterion.quality_info.quality_score,
        ad_group.campaign,
        ad_group.cpc_bid_micros,
        campaign.start_date,
        campaign.resource_name,
        campaign.name,
        campaign.id,
        ad_group_criterion.status,
        ad_group_criterion.keyword.text,
        ad_group.name,
        campaign.status,
        ad_group.id
    FROM keyword_view
    WHERE
        segments.date BETWEEN """ + start_date + " AND " + end_date + \
    " ORDER BY metrics.impressions DESC"


    # query = """
    #     SELECT
    #       campaign.id,
    #       campaign.name,
    #       ad_group.id,
    #       ad_group.name,
    #       ad_group_criterion.criterion_id,
    #       ad_group_criterion.keyword.text,
    #       ad_group_criterion.keyword.match_type,
    #       metrics.impressions,
    #       metrics.clicks,
    #       metrics.cost_micros,
    #       metrics.average_cpm,
    #       metrics.ctr,
    #       metrics.benchmark_average_max_cpc,
    #       metrics.historical_quality_score,
    #       metrics.search_impression_share,
    #       metrics.search_click_share,
    #       metrics.bounce_rate,
    #       metrics.all_conversions
    #     FROM keyword_view WHERE segments.date BETWEEN '2021-06-29' AND '2021-06-30'
    #     AND campaign.advertising_channel_type = 'SEARCH'
    #     AND ad_group.status = 'ENABLED'
    #     AND ad_group_criterion.status IN ('ENABLED', 'PAUSED')
    #     ORDER BY metrics.impressions DESC"""

    # Issues a search request using streaming.

    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = customer_id
    search_request.query = query
    response = ga_service.search_stream(search_request)
    rowcount = 0
    for batch in response:
        for row in batch.results:
            rowcount = rowcount + 1
            print(rowcount)
            data = {}
            campaign = row.campaign
            ad_group = row.ad_group
            criterion = row.ad_group_criterion
            metrics = row.metrics
            ad_group_criterion = row.ad_group_criterion
            keyword_view = row.keyword_view
            data["keyword_view_resource_name"] = keyword_view.resource_name
            data["keyword_text"] = criterion.keyword.text
            data["campaign_name"] = campaign.name
            data["ad_group_name"] = ad_group.name
            data["campaign_status"] = campaign.status
            data["ad_group_criterion_status"] = ad_group_criterion.status
            data["ad_group_campaign"] =ad_group.campaign
            data["impressions"] = metrics.impressions
            data["max_cpc"] = ad_group.cpc_bid_micros
            data["CTR"] = metrics.ctr
            data["QualityScore"] = ad_group_criterion.quality_info.quality_score
            data["search_impression_share"] = metrics.search_impression_share
            data["search_click_share"] = metrics.search_click_share
            data["bounce_rate"] = metrics.bounce_rate
            data["all_conversions"] = metrics.all_conversions
            data["ad_group_id"] = ad_group.id
            data["campaign_id"] = campaign.id
            data["clicks"] = metrics.clicks
            data["cost_micros"] = metrics.cost_micros
            data["average_cpc"] = metrics.average_cpc
            data["campaign_start_date"] = campaign.start_date
            data["campaign_resource_name"] =campaign.resource_name
            # print(data)

            if campaign.name not in uniqueCampaignList:
                uniqueCampaignList.append(campaign.name)



            dataList.append(data)


    infoDf = pd.DataFrame(dataList)
    infoDf["ad_group_name"] = infoDf["ad_group_name"].str.replace("^[0-9]* ", "", regex =True)
    finalinfoDf = pd.merge(infoDf, categoryDf, left_on="ad_group_name", right_on="ID & Ad Group", how='outer')
    finalinfoDf = finalinfoDf.loc[~finalinfoDf.campaign_name.isna(), :]
    finalinfoDf = finalinfoDf.drop(["ID & Ad Group", "Unnamed: 0"], axis=1)
    finalinfoDf.loc[finalinfoDf.Category.isna(), "Category"] = "Other"
    finalinfoDf.campaign_name = finalinfoDf.campaign_name.str.replace(" - ", "#", regex = True)
    finalinfoDf['primaryCampaignName'] = finalinfoDf[['campaign_name', 'Category']].apply(lambda x: '#'.join(x), axis=1)
    finalinfoDf = finalinfoDf.drop_duplicates()
    # for index, row in finalinfoDf.iterrows():
    #     print(row["campaign_name"]+"#"+row["Category"])

    # for uniquecamp in finalinfoDf.primaryCampaignName.unique():
    #     campnew.create_campaign(campaign_name=uniquecamp,
    #                             campaign_budget_value=500000,
    #                             campaign_budget_name=uniquecamp + "#Budget")

    # finalinfoDf.to_csv("finalinfoDf.csv")
    eternalLoop(finalinfoDf, campnew)


    # for eachcamp in uniqueCampaignList:
    #     campaignNameData = {}
    #     campaignNameData["state"] = eachcamp.split(" - ")[0]
    #     if len(campaignNameData["state"]) > 0:
    #         campaignNameData["location"] = eachcamp.split(" - ")[1].split(" #")[0]
    #         campaignNameData["businessUnit"] = eachcamp.split(" - ")[1].split(" #")[1]
    #     else:
    #         campaignNameData["location"] = np.nan
    #         campaignNameData["businessUnit"] = np.nan
    #     campaignNameDataDataFrame.append(campaignNameData)
    #
    #     pd.DataFrame(campaignNameDataDataFrame).to_csv("campaignNameDataDataFrame.csv")



        # df = pd.DataFrame(dataList)
        # df.to_csv("DemoData.csv")

    # [END get_keyword_stats]


def eternalLoop(df, campnew):

    output = pd.DataFrame()
    alreadyCreatedCampaign = {}
    print("ENTERED eternalLoop")
    try:
        for eachcampaign in df.primaryCampaignName.unique():
            print("Making calculations for ad_group_name")
            unique_ad_group_list = df.loc[df.primaryCampaignName == eachcampaign, "ad_group_name"].unique()
            for each_ad_group in unique_ad_group_list:
                overallTocreatedict = {}
                print("Calculating quantiles")
                _, low, mid, high = df.loc[(df.primaryCampaignName == eachcampaign) &
                                           (df.ad_group_name == each_ad_group), "clicks"].quantile([0, 0.20, 0.40, 1])
                if low == mid == high:
                    overallTocreatedict["campaign_name"] = eachcampaign
                    overallTocreatedict["ad_group_name"] = each_ad_group + "#Common"
                    overallTocreatedict["KeywordList"] = list(df.loc[(df.primaryCampaignName == eachcampaign) &
                                                                     (df.ad_group_name == each_ad_group), "keyword_text"].values)
                    overallTocreatedict["Bid"] = 500000
                    overallTocreatedict["campaign_budget_name"] = overallTocreatedict["campaign_name"] + "#Budget"
                    if overallTocreatedict["campaign_name"] not in alreadyCreatedCampaign.keys():
                        overallTocreatedict["campaign_id"] = \
                            campnew.create_campaign(campaign_budget_name= overallTocreatedict["campaign_budget_name"],
                                                    campaign_budget_value=50000000,
                                                    campaign_name=overallTocreatedict["campaign_name"])["campaign_id"]
                        overallTocreatedict["campaign_created"] = True
                        alreadyCreatedCampaign[overallTocreatedict["campaign_name"]] = overallTocreatedict["campaign_id"]
                    else:
                        overallTocreatedict["campaign_created"] = True
                        overallTocreatedict["campaign_id"] = alreadyCreatedCampaign[overallTocreatedict["campaign_name"]]
                    overallTocreatedict["group_id"] = campnew.create_ad_group(campaign_id=overallTocreatedict["campaign_id"],
                                                                      ad_group_name=overallTocreatedict["ad_group_name"],
                                                                      bid_amount=overallTocreatedict["Bid"])["ad_group_id"]
                    overallTocreatedict["Add_group_created"] = True
                    for keyword in overallTocreatedict["KeywordList"]:
                        campnew.add_keywords(ad_group_id=overallTocreatedict["group_id"],
                                             keyword_text=keyword)
                    print(overallTocreatedict)
                    output = output.append(overallTocreatedict, ignore_index=True)

                else:
                    overallTocreatedict = {}
                    lowList = list(df.loc[(df.primaryCampaignName == eachcampaign) &
                                          (df.ad_group_name == each_ad_group) &
                                          (df.clicks <= low), "keyword_text"].values)
                    midList = list(df.loc[(df.primaryCampaignName == eachcampaign) &
                                          (df.ad_group_name == each_ad_group) &
                                          (df.clicks > low) & (df.clicks <= mid), "keyword_text"].values)
                    highList = list(df.loc[(df.primaryCampaignName == eachcampaign) &
                                           (df.ad_group_name == each_ad_group) &
                                           (df.clicks > mid), "keyword_text"].values)

                    if len(highList) > 0:
                        overallTocreatedict["campaign_name"] = eachcampaign
                        overallTocreatedict["ad_group_name"] = each_ad_group + "#High"
                        overallTocreatedict["KeywordList"] = highList
                        overallTocreatedict["Bid"] = 1200000
                        overallTocreatedict["campaign_budget_name"] = overallTocreatedict["campaign_name"] + "#Budget"
                        if overallTocreatedict["campaign_name"] not in alreadyCreatedCampaign.keys():
                            overallTocreatedict["campaign_id"] = \
                                campnew.create_campaign(campaign_budget_name=overallTocreatedict["campaign_budget_name"],
                                                        campaign_budget_value=50000000,
                                                        campaign_name=overallTocreatedict["campaign_name"])["campaign_id"]
                            overallTocreatedict["campaign_created"] = True
                            alreadyCreatedCampaign[overallTocreatedict["campaign_name"]] = overallTocreatedict["campaign_id"]
                        else:
                            overallTocreatedict["campaign_created"] = True
                            overallTocreatedict["campaign_id"] = alreadyCreatedCampaign[overallTocreatedict["campaign_name"]]

                        overallTocreatedict["group_id"] = campnew.create_ad_group(campaign_id=overallTocreatedict["campaign_id"],
                                                                          ad_group_name=overallTocreatedict["ad_group_name"],
                                                                          bid_amount=overallTocreatedict["Bid"])["ad_group_id"]
                        overallTocreatedict["Add_group_created"] = True
                        print(overallTocreatedict)
                        output = output.append(overallTocreatedict, ignore_index=True)

                        for keyword in overallTocreatedict["KeywordList"]:
                            campnew.add_keywords(ad_group_id=overallTocreatedict["group_id"],
                                                 keyword_text=keyword)

                    if len(midList) > 0:
                        overallTocreatedict["campaign_name"] = eachcampaign
                        overallTocreatedict["ad_group_name"] = each_ad_group + "#Mid"
                        overallTocreatedict["KeywordList"] = midList
                        overallTocreatedict["Bid"] = 600000
                        overallTocreatedict["campaign_budget_name"] = overallTocreatedict["campaign_name"] + "#Budget"
                        if overallTocreatedict["campaign_name"] not in alreadyCreatedCampaign.keys():
                            overallTocreatedict["campaign_id"] = \
                            campnew.create_campaign(campaign_budget_name=overallTocreatedict["campaign_budget_name"],
                                            campaign_budget_value=50000000,
                                            campaign_name=overallTocreatedict["campaign_name"])["campaign_id"]
                            overallTocreatedict["campaign_created"] = True
                            alreadyCreatedCampaign[overallTocreatedict["campaign_name"]] = overallTocreatedict["campaign_id"]
                        else:
                            overallTocreatedict["campaign_created"] = True
                            overallTocreatedict["campaign_id"] = alreadyCreatedCampaign[overallTocreatedict["campaign_name"]]

                        overallTocreatedict["group_id"] = campnew.create_ad_group(campaign_id=overallTocreatedict["campaign_id"],
                                                                          ad_group_name=overallTocreatedict["ad_group_name"],
                                                                          bid_amount=overallTocreatedict["Bid"])["ad_group_id"]
                        overallTocreatedict["Add_group_created"] = True
                        print(overallTocreatedict)
                        output = output.append(overallTocreatedict, ignore_index=True)


                        for keyword in overallTocreatedict["KeywordList"]:
                            campnew.add_keywords(ad_group_id = overallTocreatedict["group_id"],
                                                 keyword_text = keyword)

                    if len(lowList) > 0:

                        overallTocreatedict["campaign_name"] = eachcampaign
                        overallTocreatedict["ad_group_name"] = each_ad_group + "#Low"
                        overallTocreatedict["KeywordList"] = lowList
                        overallTocreatedict["Bid"] = 500000
                        overallTocreatedict["campaign_budget_name"] = overallTocreatedict["campaign_name"] + "#Budget"
                        if overallTocreatedict["campaign_name"] not in alreadyCreatedCampaign.keys():
                            overallTocreatedict["campaign_id"] = \
                            campnew.create_campaign(campaign_budget_name=overallTocreatedict["campaign_budget_name"],
                                            campaign_budget_value=50000000,
                                            campaign_name=overallTocreatedict["campaign_name"])["campaign_id"]
                            overallTocreatedict["campaign_created"] = True
                            alreadyCreatedCampaign[overallTocreatedict["campaign_name"]] = overallTocreatedict["campaign_id"]
                        else:
                            overallTocreatedict["campaign_created"] = True
                            overallTocreatedict["campaign_id"] = alreadyCreatedCampaign[overallTocreatedict["campaign_name"]]

                        overallTocreatedict["group_id"] = campnew.create_ad_group(campaign_id=overallTocreatedict["campaign_id"],
                                                                          ad_group_name=overallTocreatedict["ad_group_name"],
                                                                          bid_amount=overallTocreatedict["Bid"])["ad_group_id"]

                        overallTocreatedict["Add_group_created"] = True
                        print(overallTocreatedict)
                        output = output.append(overallTocreatedict, ignore_index=True)


                        for keyword in overallTocreatedict["KeywordList"]:
                            campnew.add_keywords(ad_group_id=overallTocreatedict["group_id"],
                                                 keyword_text=keyword)



        output.to_csv("output.csv")
    except:
        print("GOT ERROR SAVING RESULTS")
        output.to_csv("output.csv")

if __name__ == "__main__":

    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage("googleads.yaml")
    newClient = "9814114286"
    oldClient = "9025864929"
    camp = campaign()
    campnew = campaign(customer_id="9814114286")
    maincode(client=googleads_client,
             customer_id=oldClient,
             start_date="'2021-06-30'",
             end_date="'2021-07-02'",
             camp=camp, campnew=campnew)
