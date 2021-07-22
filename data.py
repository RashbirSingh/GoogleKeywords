import numpy as np
from google.ads.googleads.client import GoogleAdsClient
from campaignControl import campaign as CCL
import pandas as pd


def maincode(client, customer_id, camp, campnew,
             start_date="'2021-01-01'", end_date="'2021-06-30'",
             setlimit=0):
    dataList = []

    uniqueCampaignList = []
    ga_service = client.get_service("GoogleAdsService")

    if setlimit != 0:
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
        ad_group_criterion.position_estimates.top_of_page_cpc_micros,
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
                " ORDER BY metrics.impressions DESC LIMIT " + str(setlimit)
    else:
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
        ad_group_criterion.position_estimates.top_of_page_cpc_micros,
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
                " ORDER BY metrics.impressions DESC"""

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
            data["ad_group_campaign"] = ad_group.campaign
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
            data["campaign_resource_name"] = campaign.resource_name
            data["top_of_page_cpc_micros"] = ad_group_criterion.position_estimates.top_of_page_cpc_micros

            if campaign.name not in uniqueCampaignList:
                uniqueCampaignList.append(campaign.name)

            dataList.append(data)

    infoDf = pd.DataFrame(dataList)

    finalinfoDf = infoDf.loc[~infoDf.campaign_name.isna(), :]
    finalinfoDf.campaign_name = finalinfoDf.campaign_name.str.replace(" - ", "#", regex=True)
    finalinfoDf['primaryCampaignName'] = finalinfoDf['campaign_name']
    finalinfoDf = finalinfoDf.drop_duplicates()

    finalinfoDf.to_csv("finalinfoDf.csv")
    eternalLoop(finalinfoDf, campnew)


def eternalLoop(df, campnew):
    output = pd.DataFrame()
    alreadyCreatedCampaign = {}
    print("ENTERED eternalLoop")

    for eachcampaign in df.primaryCampaignName.unique():
        print("Making calculations for ad_group_name")
        unique_ad_group_list = df.loc[df.primaryCampaignName == eachcampaign, "ad_group_name"].unique()
        for each_ad_group in unique_ad_group_list:

            clicksNintyPercentile = df.loc[(df.primaryCampaignName == eachcampaign) &
                                           (df.ad_group_name == each_ad_group) &
                                           (df.clicks > 0), "clicks"].quantile(0.90)
            impressionsNintyPercentile = df.loc[(df.primaryCampaignName == eachcampaign) &
                                                (df.ad_group_name == each_ad_group) &
                                                (df.impressions > 0), "impressions"].quantile(0.90)
            CTRFiftyPercentile = df.loc[(df.primaryCampaignName == eachcampaign) &
                                        (df.ad_group_name == each_ad_group) &
                                        (df.CTR > 0), "CTR"].quantile(0.50)

            overallTocreatedict = {}

            universeDataFrame = df.loc[(df.primaryCampaignName == eachcampaign) &
                                       (df.ad_group_name == each_ad_group), ["keyword_text",
                                                                             "top_of_page_cpc_micros",
                                                                             "average_cpc"]]

            if df.loc[(df.primaryCampaignName == eachcampaign) &
                      (df.ad_group_name == each_ad_group) &
                      (df.QualityScore > 0), "keyword_text"].shape[0] > 0:

                ##--------------------------------------------------HIGH LIST----------------------------------------------------------
                featureRichHighDataframe = df.loc[(df.primaryCampaignName == eachcampaign) &
                                                  (df.ad_group_name == each_ad_group) &
                                                  (df.QualityScore > 7), ["keyword_text",
                                                                          "top_of_page_cpc_micros",
                                                                          "average_cpc"]]
                highDataFrame = featureRichHighDataframe.keyword_text
                highList = highDataFrame.shape[0]
                ##--------------------------------------------------MID LIST----------------------------------------------------------
                featureRichMidDataFrame = df.loc[(df.primaryCampaignName == eachcampaign) &
                                                 (df.ad_group_name == each_ad_group) &
                                                 (df["QualityScore"] > 5) & (df["QualityScore"] <= 7), ["keyword_text",
                                                                                                        "top_of_page_cpc_micros",
                                                                                                        "average_cpc"]]
                midDataframe = featureRichMidDataFrame.keyword_text
                midList = midDataframe.shape[0]
                ##--------------------------------------------------LOW LIST----------------------------------------------------------
                union = pd.Series(np.union1d(universeDataFrame.keyword_text, highDataFrame))
                intersect = pd.Series(np.intersect1d(universeDataFrame.keyword_text, highDataFrame))
                lowDataframe = union[~union.isin(intersect)]
                union = pd.Series(np.union1d(lowDataframe, midDataframe))
                intersect = pd.Series(np.intersect1d(lowDataframe, midDataframe))
                lowDataframe = union[~union.isin(intersect)]

                lowList = lowDataframe.shape[0]
                featureRichLowDataFrame = universeDataFrame[~universeDataFrame.index.isin(lowDataframe.index)]

            else:
                ##--------------------------------------------------HIGH LIST----------------------------------------------------------
                featureRichHighDataframe = df.loc[(df.primaryCampaignName == eachcampaign) &
                                                  (df.ad_group_name == each_ad_group) &
                                                  (df.clicks > clicksNintyPercentile) &
                                                  (df.impressions > impressionsNintyPercentile) &
                                                  (df.CTR > CTRFiftyPercentile), ["keyword_text",
                                                                                  "top_of_page_cpc_micros",
                                                                                  "average_cpc"]]
                highDataFrame = featureRichHighDataframe.keyword_text
                highList = highDataFrame.shape[0]
                ##--------------------------------------------------MID LIST----------------------------------------------------------
                featureRichMidDataFrame = df.loc[(df.primaryCampaignName == eachcampaign) &
                                                 (df.ad_group_name == each_ad_group) &
                                                 (df.clicks <= clicksNintyPercentile) &
                                                 (df.clicks > 0) &
                                                 (df.impressions <= impressionsNintyPercentile) &
                                                 (df.impressions > 0) &
                                                 (df.CTR < CTRFiftyPercentile) &
                                                 (df.CTR > 0), ["keyword_text", "top_of_page_cpc_micros",
                                                                "average_cpc"]]
                midDataframe = featureRichMidDataFrame.keyword_text
                midList = midDataframe.shape[0]
                ##--------------------------------------------------LOW LIST----------------------------------------------------------
                union = pd.Series(np.union1d(universeDataFrame.keyword_text, highDataFrame))
                intersect = pd.Series(np.intersect1d(universeDataFrame.keyword_text, highDataFrame))
                lowDataframe = union[~union.isin(intersect)]
                union = pd.Series(np.union1d(lowDataframe, midDataframe))
                intersect = pd.Series(np.intersect1d(lowDataframe, midDataframe))
                lowDataframe = union[~union.isin(intersect)]

                lowList = lowDataframe.shape[0]
                featureRichLowDataFrame = universeDataFrame.loc[universeDataFrame.index.isin(lowDataframe.index), :]

            ##--------------------------------------------------ASSESING HIGH LIST----------------------------------------------------------
            if highList > 0:
                overallTocreatedict["campaign_name"] = eachcampaign
                overallTocreatedict["ad_group_name"] = each_ad_group + "#High"
                overallTocreatedict["KeywordList"] = highDataFrame.tolist()

                bidValue = int(universeDataFrame.top_of_page_cpc_micros.mean()) - (
                        int(universeDataFrame.top_of_page_cpc_micros.mean()) % 10000)
                if (bidValue == 0) or (bidValue < 10000):
                    bidValue = 10000
                overallTocreatedict["Bid"] = bidValue
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

                overallTocreatedict["group_id"] = \
                    campnew.create_ad_group(campaign_id=overallTocreatedict["campaign_id"],
                                            ad_group_name=overallTocreatedict["ad_group_name"],
                                            bid_amount=overallTocreatedict["Bid"])["ad_group_id"]
                overallTocreatedict["Add_group_created"] = True
                print(overallTocreatedict)
                output = output.append(overallTocreatedict, ignore_index=True)

                for keyword in overallTocreatedict["KeywordList"]:
                    try:
                        campnew.add_keywords(ad_group_id=overallTocreatedict["group_id"],
                                             keyword_text=keyword)
                    except:
                        print("Some error")
                        pass

            ##--------------------------------------------------ASSESING MID LIST----------------------------------------------------------
            if midList > 0:
                overallTocreatedict["campaign_name"] = eachcampaign
                overallTocreatedict["ad_group_name"] = each_ad_group + "#Mid"
                overallTocreatedict["KeywordList"] = midDataframe.tolist()

                bidValue = (int(universeDataFrame.top_of_page_cpc_micros.mean()) - (
                        int(universeDataFrame.top_of_page_cpc_micros.mean()) % 10000)) * 0.75
                bidValue = int(bidValue) - int(bidValue % 10000)
                if (bidValue == 0) or (bidValue < 10000):
                    bidValue = 10000
                elif bidValue > 3000000:
                    bidValue = 3000000

                overallTocreatedict["Bid"] = bidValue
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

                overallTocreatedict["group_id"] = \
                    campnew.create_ad_group(campaign_id=overallTocreatedict["campaign_id"],
                                            ad_group_name=overallTocreatedict["ad_group_name"],
                                            bid_amount=overallTocreatedict["Bid"])["ad_group_id"]
                overallTocreatedict["Add_group_created"] = True
                print(overallTocreatedict)
                output = output.append(overallTocreatedict, ignore_index=True)

                for keyword in overallTocreatedict["KeywordList"]:
                    try:
                        campnew.add_keywords(ad_group_id=overallTocreatedict["group_id"],
                                             keyword_text=keyword)
                    except:
                        print("Some error")
                        pass

            ##--------------------------------------------------ASSESING LOW LIST----------------------------------------------------------
            if lowList > 0:

                overallTocreatedict["campaign_name"] = eachcampaign
                overallTocreatedict["ad_group_name"] = each_ad_group + "#Low"
                overallTocreatedict["KeywordList"] = lowDataframe.tolist()

                threshold = ((int(universeDataFrame.top_of_page_cpc_micros.mean()) - (
                        int(universeDataFrame.top_of_page_cpc_micros.mean()) % 10000)) * 0.75) * 0.33

                avgLowCPC = featureRichLowDataFrame.average_cpc.mean()

                if avgLowCPC > threshold:
                    bidValue = threshold

                bidValue = int(bidValue) - int(bidValue % 10000)

                if (bidValue == 0) or (bidValue < 10000):
                    bidValue = 10000
                elif bidValue >= 3000000:
                    bidValue = 3000000

                overallTocreatedict["Bid"] = bidValue
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

                overallTocreatedict["group_id"] = \
                    campnew.create_ad_group(campaign_id=overallTocreatedict["campaign_id"],
                                            ad_group_name=overallTocreatedict["ad_group_name"],
                                            bid_amount=overallTocreatedict["Bid"])["ad_group_id"]

                overallTocreatedict["Add_group_created"] = True
                print(overallTocreatedict)
                output = output.append(overallTocreatedict, ignore_index=True)

                for keyword in overallTocreatedict["KeywordList"]:
                    try:
                        campnew.add_keywords(ad_group_id=overallTocreatedict["group_id"],
                                             keyword_text=keyword)
                    except:
                        print("Some error")
                        pass

    output.to_csv("output.csv")


if __name__ == "__main__":
    googleads_client = GoogleAdsClient.load_from_storage("googleads.yaml")
    newClient = "9814114286"
    oldClient = "9025864929"
    camp = CCL()
    campnew = CCL(customer_id="9814114286")
    maincode(client=googleads_client,
             customer_id=oldClient,
             start_date="'2021-03-01'",
             end_date="'2021-07-21'",
             camp=camp, campnew=campnew,
             setlimit=0)

