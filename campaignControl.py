from google.ads.googleads.client import GoogleAdsClient
import datetime
from dotenv import dotenv_values
config = dotenv_values(".env")

class campaign:

    def __init__(self, client_cred_path = "googleads.yaml",
                 customer_id="9025864929"):

        self.client = GoogleAdsClient.load_from_storage(client_cred_path)
        self.customer_id = customer_id


    def create_campaign(self,
                        campaign_budget_name = "DemoTestBudget",
                        campaign_budget_value = 500000,
                        campaign_name = "DemoCampaign"):

        _DATE_FORMAT = "%Y%m%d"
        data = {}
        client = self.client
        customer_id = self.customer_id
        campaign_budget_service = client.get_service("CampaignBudgetService")
        campaign_service = client.get_service("CampaignService")
        campaign_budget_operation = client.get_type("CampaignBudgetOperation")
        campaign_budget = campaign_budget_operation.create
        campaign_budget.name = campaign_budget_name
        campaign_budget.delivery_method = client.get_type(
            "BudgetDeliveryMethodEnum"
        ).BudgetDeliveryMethod.STANDARD
        campaign_budget.amount_micros = campaign_budget_value

        campaign_budget_response = campaign_budget_service.mutate_campaign_budgets(
            customer_id=customer_id, operations=[campaign_budget_operation])

        # Create campaign.
        campaign_operation = client.get_type("CampaignOperation")
        campaign = campaign_operation.create
        campaign.name = campaign_name
        campaign.advertising_channel_type = client.get_type(
            "AdvertisingChannelTypeEnum"
        ).AdvertisingChannelType.SEARCH

        # Recommendation: Set the campaign to PAUSED when creating it to prevent
        # the ads from immediately serving. Set to ENABLED once you've added
        # targeting and the ads are ready to serve.
        campaign.status = client.get_type(
            "CampaignStatusEnum"
        ).CampaignStatus.PAUSED

        # Set the bidding strategy and budget.
        campaign.manual_cpc.enhanced_cpc_enabled = True
        campaign.campaign_budget = campaign_budget_response.results[0].resource_name

        # Set the campaign network options.
        campaign.network_settings.target_google_search = True
        campaign.network_settings.target_search_network = True
        campaign.network_settings.target_content_network = False
        campaign.network_settings.target_partner_search_network = False

        # Optional: Set the start date.
        start_time = datetime.date.today() + datetime.timedelta(days=1)
        campaign.start_date = datetime.date.strftime(start_time, _DATE_FORMAT)

        # Optional: Set the end date.
        end_time = start_time + datetime.timedelta(weeks=4)
        campaign.end_date = datetime.date.strftime(end_time, _DATE_FORMAT)

        # Add the campaign.
        campaign_response = campaign_service.mutate_campaigns(
            customer_id=customer_id, operations=[campaign_operation]
        )
        campaign_data = campaign_response.results[0].resource_name.split('/')
        data["campaign_data"] = campaign_data
        data["customer_id"] = campaign_data[1]
        data["campaign_id"] = campaign_data[3]
        return data


    def create_ad_group(self,
                        campaign_id,
                        ad_group_name = "DemoAddGroup",
                        bid_amount = 10000000):
        data = {}
        client = self.client
        customer_id = self.customer_id
        ad_group_service = client.get_service("AdGroupService")
        campaign_service = client.get_service("CampaignService")

        # Create ad group.
        ad_group_operation = client.get_type("AdGroupOperation")
        ad_group = ad_group_operation.create
        ad_group.name = ad_group_name
        ad_group.status = client.get_type("AdGroupStatusEnum").AdGroupStatus.ENABLED
        ad_group.campaign = campaign_service.campaign_path(customer_id, campaign_id)
        ad_group.type_ = client.get_type(
            "AdGroupTypeEnum"
        ).AdGroupType.SEARCH_STANDARD
        ad_group.cpc_bid_micros = bid_amount

        # Add the ad group.
        ad_group_response = ad_group_service.mutate_ad_groups(
            customer_id=customer_id, operations=[ad_group_operation]
        )

        data["ad_group_data"] = ad_group_response.results[0].resource_name
        data["ad_group_id"] = ad_group_response.results[0].resource_name.split("/")[-1]

        return data

    def add_keywords(self, ad_group_id, keyword_text):
        client = self.client
        customer_id = self.customer_id
        ad_group_service = client.get_service("AdGroupService")
        ad_group_criterion_service = client.get_service("AdGroupCriterionService")

        # Create keyword.
        ad_group_criterion_operation = client.get_type("AdGroupCriterionOperation")
        ad_group_criterion = ad_group_criterion_operation.create
        ad_group_criterion.ad_group = ad_group_service.ad_group_path(
            customer_id, ad_group_id
        )
        ad_group_criterion.status = client.get_type(
            "AdGroupCriterionStatusEnum"
        ).AdGroupCriterionStatus.ENABLED
        ad_group_criterion.keyword.text = keyword_text
        ad_group_criterion.keyword.match_type = client.get_type(
            "KeywordMatchTypeEnum"
        ).KeywordMatchType.BROAD

        # Optional field
        # All fields can be referenced from the protos directly.
        # The protos are located in subdirectories under:
        # https://github.com/googleapis/googleapis/tree/master/google/ads/googleads
        # ad_group_criterion.negative = True

        # Optional repeated field
        ad_group_criterion.final_urls.append('https://www.staffing.com.au')

        # Add keyword
        ad_group_criterion_response = ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id, operations=[ad_group_criterion_operation],
        )

        # print(
        #     "Created keyword "
        #     f"{ad_group_criterion_response.results[0].resource_name}."
        # )