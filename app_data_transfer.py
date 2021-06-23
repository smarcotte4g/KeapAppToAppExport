import json
import os
from mysql.connector.fabric.connection import FabricSet
import pandas as pd
import re
import datetime as dt
import sys
from pandas.tseries.offsets import DateOffset
# from pprint import pprint
#import numpy as np

from models import Database

def export_contacts(source, tag_id):
    contacts = source.get_table('Contact')
    custom_contacts = source.get_table('Custom_Contact')

    # Filter contacts to extract
    contacts = contacts.loc[contacts.IsUser == 0
                            & ~(contacts.Id != contacts.CompanyID)]
    
    if tag_id:
        tag_apps = source.get_table('ContactGroupAssign')
        f_tag_apps = tag_apps[tag_apps['GroupId'] == tag_id]
        contact_ids_to_keep = f_tag_apps['ContactId'].tolist()
        contacts = contacts[contacts['Id'].isin(contact_ids_to_keep)]
    
    # Clean that frame up: This will be removed for db transfer
    contacts.drop(['OffSetTimeZone','ZipFour1','Validated','Groups','ZipFour2','ZipFour3','LastUpdatedBy','IsUser','LastUpdatedUtcMillis','BrowserLanguage','TimeZone','LanguageTag','SourceType','InternalDateCreated','LastEmailed'], axis=1, inplace=True)
    
    contacts = pd.merge(contacts, custom_contacts, on='Id', how='left')

    return contacts

def export_companies(source, company_ids):
    companies = source.get_table('Contact')
    custom_company = source.get_table('Custom_Company')

    # Filter contacts to extract
    companies = companies.loc[(companies['IsUser'] == 0) & (companies['Id'] == companies['CompanyID'])]
    
    # Clean that frame up: This will be removed for db transfer
    companies.drop(['FirstName','LastName','Phone1Ext','Phone2Type','Phone2','Phone2Ext','Phone3Type','Phone3','Phone3Ext','Username','Password','Title','ReferralCode','MiddleName','Suffix','Nickname','JobTitle','Address1Type','Address2Street1','Address2Street2','City2','State2','PostalCode2','Country2','Address2Type','Birthday','EmailAddress2','SpouseName','EmailAddress3','Address3Type','Address3Street1','Address3Street2','City3','State3','PostalCode3','Country3','AssistantName','AssistantPhone','Phone4Type','Phone4','Phone4Ext','Phone5Type','Phone5','Phone5Ext','BillingInformation','Fax2','Fax2Type','Anniversary','ContactType','SSN','SSNBackup','Last4SSN','OffSetTimeZone','ZipFour1','Validated','Groups','ZipFour2','ZipFour3','LastUpdatedBy','IsUser','LastUpdatedUtcMillis','BrowserLanguage','TimeZone','LanguageTag','SourceType','InternalDateCreated','LastEmailed'], axis=1, inplace=True)
    
    companies = pd.merge(companies, custom_company, on='Id', how='left')

    if company_ids:
        companies = companies[companies['CompanyID'].isin(company_ids)]

    return companies

def export_tags(source, tag_ids):
    s_tags = source.get_table('ContactGroup')
    s_tc = source.get_table('ContactGroupCategory')
    
    # Clean that frame up: This will be removed for db transfer
    s_tags.drop(['ImportId','ContentPublishingId'], axis=1, inplace=True)
    s_tc.drop(['ImportId','ContentPublishingId','DateCreated','LastUpdated'], axis=1, inplace=True)

    # Rename column to do easy merge with categories
    s_tc.rename({'Id': 'GroupCategoryId'}, axis=1, inplace=True)
    tags = pd.merge(s_tags,s_tc, how='left', on=['GroupCategoryId'])

    if tag_ids:
        tags = tags[tags['Id'].isin(tag_ids)]

    return tags

def export_tag_applicaitons(source, contact_ids):
    s_tag_apps = source.get_table('ContactGroupAssign')

    # Clean that frame up: This will be removed for db transfer
    s_tag_apps.drop(['DateRemoved','CurrentStatus','LastUpdated'], axis=1, inplace=True)

    if contact_ids:
        s_tag_apps = s_tag_apps[s_tag_apps['ContactId'].isin(contact_ids)]

    return s_tag_apps

def export_opportunities(source, contact_ids):
    opps = source.get_table('Opportunity')
    custom_opps = source.get_table('Custom_Opportunity')

    # Clean that frame up: This will be removed for db transfer
    opps.drop(['TimeCreated','SessionID','CreatedBy','LastUpdatedBy','Objection','SalesProcessId','GlobalCampaignId','IncludeInForecast','FreeTrialDays'], axis=1, inplace=True)

    opps = pd.merge(opps, custom_opps, on='Id', how='left')
    if contact_ids:
        opps = opps[opps['ContactID'].isin(contact_ids)]

    return opps

def export_stages(source):
    stage = source.get_table('Stage', columns=['Id','StageName','StageOrder','TargetNumDays','Probability'])

    return stage

def export_products(source, product_ids):
    prods = source.get_table('Product')
    custom_prods = source.get_table('Custom_Product')

    # Clean that frame up: This will be removed for db transfer
    prods.drop(['SubCategory','ProductDate','PercentSavings','ProductImagePath','Favorite','VendorURL','Type','Family','ProdMfrID','SizeTypeID','IsPackage','LeadAmt','LeadPercent','SaleAmt','SalePercent','PayoutType','PurchasedContactTemplate','ProductImageLarge','ProductImageSmall','ShippingTime','InventoryNotifiee','InventoryEmailSent','BottomHTML','TopHTML','DigitalTemplateId','DownloadHeader','DownloadFooter','DownloadLength','LegacyCProgramId'], axis=1, inplace=True)

    prods = pd.merge(prods, custom_prods, on='Id', how='left')
    if product_ids:
        prods = prods[prods['Id'].isin(product_ids)]

    return prods

def export_notes(source, contact_ids):
    notes = source.get_table('ContactAction')

    notes = notes.loc[(notes['ObjectType'] == 'Note')]

    # Clean that frame up: This will be removed for db transfer
    notes.drop(['OpportunityId','CreationDate','CompletionDate','EndDate','PopupDate','Accepted','CreatedBy','RemindTime','IsAppointment','LastUpdatedBy','LastUpdated','RecurrenceId','TemplateId','CompletionScenarioId','FunnelId','JGraphId','TaskStrategy','Location','ActionDateUtc','RemindedDate','CompletionDateOld','ActionDateOld','EndDateOld','PopupDateOld'], axis=1, inplace=True)

    if contact_ids:
        notes = notes[notes['ContactId'].isin(contact_ids)]

    return notes

def export_tasks(source, contact_ids):
    tasks = source.get_table('ContactAction')

    tasks = tasks.loc[(tasks['ObjectType'] == 'Task')]

    # Clean that frame up: This will be removed for db transfer
    tasks.drop(['OpportunityId','CreationDate','EndDate','PopupDate','Accepted','CreatedBy','RemindTime','IsAppointment','LastUpdatedBy','LastUpdated','RecurrenceId','TemplateId','CompletionScenarioId','FunnelId','JGraphId','TaskStrategy','Location','DateCreated','ActionDateUtc','RemindedDate','CompletionDateOld','ActionDateOld','EndDateOld','PopupDateOld'], axis=1, inplace=True)

    if contact_ids:
        tasks = tasks[tasks['ContactId'].isin(contact_ids)]

    return tasks
    
def export_appointments(source, contact_ids):
    appointments = source.get_table('ContactAction')

    appointments = appointments.loc[(appointments['ObjectType'] == 'Appointment')]

    # Clean that frame up: This will be removed for db transfer
    appointments.drop(['OpportunityId','CreationDate','CompletionDate','PopupDate','Accepted','CreatedBy','RemindTime','IsAppointment','LastUpdatedBy','LastUpdated','RecurrenceId','TemplateId','CompletionScenarioId','FunnelId','JGraphId','TaskStrategy','Location','DateCreated','ActionDateUtc','RemindedDate','CompletionDateOld','ActionDateOld','EndDateOld','PopupDateOld'], axis=1, inplace=True)

    if contact_ids:
        appointments = appointments[appointments['ContactId'].isin(contact_ids)]

    return appointments

##### Get Orders
def export_job(source, contact_ids):
    job = source.get_table('Job', columns=['Id','ContactId','JobTitle','JobNotes','DueDate','OrderType'])
    custom_job = source.get_table('Custom_Job')

    job = pd.merge(job, custom_job, on='Id', how='left')

    if contact_ids:
        job = job[job['ContactId'].isin(contact_ids)]

    return job

def export_order_item(source, job_ids):
    order_items = source.get_join_table('OrderItem','t1 INNER JOIN Job t2 ON t2.Id=t1.OrderId', columns=['t1.Id AS OrderItemId', 't1.OrderId AS JobId', 't1.ProductId', 't1.ItemType', 't1.ItemName', 't1.ItemDescription', 't1.Qty', 't1.PPU AS PricePerUnit', 't1.CPU AS CostPerUnit', 't1.ServiceEnd', 't1.Serial AS SerialNumber', 't1.Notes'])
    
    if job_ids:
        order_items = order_items[order_items['OrderIdJobId'].isin(job_ids)]

    return order_items

def export_payment(source, job_ids):
    payment = source.get_join_table('Payment', 't1 INNER JOIN InvoicePayment t2 ON t2.PaymentId=t1.Id INNER JOIN Invoice t3 ON t3.Id=t2.InvoiceId INNER JOIN Job t4 ON t4.Id=t3.JobId', columns=['t4.Id AS jobid', 't3.Id AS invoiceid', 't1.Id AS paymentid', 't1.PayDate', 't1.PayAmt', 't1.PayType', 't1.ContactId', 't1.PayNote'])
    
    if job_ids:
        payment = payment[payment['jobid'].isin(job_ids)]

    return payment

def export_subscriptions(source, contact_ids):
    jobrecurring = source.get_table('JobRecurring')
    custom_jobrecurring = source.get_table('Custom_JobRecurring')

    # Send each row to paid_thru_date to get output
    jobrecurring['PaidThruDate'] = jobrecurring.apply(paid_thru_date, axis=1)

    # Clean that frame up: This will be removed for db transfer
    jobrecurring.drop(['ProgramId','CC2','JobDescription','JobNotes','SendInvoices','MerchantAccountId','CommissionUntil','JumpLogId','BillingMovePending','BillingMoveOldNextBillDate','Taxable','EmailInvoiceFlag','HaveSuccessActionsRun','MarketingEmailId','Prorate','ShippingOptionId','ShippingDeliveryType','OriginatingOrderId','BillingDayOfMonth','LastUpdated','AddressId','StartDateOld','EndDateOld','LastBillDateOld','NextBillDateOld'], axis=1, inplace=True)

    jobrecurring = pd.merge(jobrecurring, custom_jobrecurring, on='Id', how='left')
    if contact_ids:
        jobrecurring = jobrecurring[jobrecurring['ContactId'].isin(contact_ids)]

    return jobrecurring

##### This will take a row from jobrecurring and add a paid thru date
def paid_thru_date(row):
    if row['BillingCycle'] == '1':
        val = row['NextBillDate'] - DateOffset(years=1)
    elif row['BillingCycle'] == '2':
        val = row['NextBillDate'] - DateOffset(months=1)
    elif row['BillingCycle'] == '3':
        val = row['NextBillDate'] - DateOffset(weeks=1)
    elif row['BillingCycle'] == '6':
        val = row['NextBillDate'] - DateOffset(days=1)
    else:
        val = row['NextBillDate'] - DateOffset(months=1)
    
    return val

