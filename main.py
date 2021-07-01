from collections import defaultdict
import json
import os
from re import S
import sys

import app_data_transfer as adt
from models import Database

def main():

    print("""
============ Keap Database Export Tool ============

This tool is for exporting and transforming data
to manually transfer to another application.

Please provide information for the source application:
    """)
    source_app = ''
    while len(source_app) <= 0:
        source_app = input('Source Application Name:\n')
    source_port = 0
    while len(str(source_port)) != 5:
        try:
            source_port = int(input('Source Application Port:\n'))
        except ValueError:
            print('Port number must be 5-digits.\n')
    tag_id = input("""
Only import contacts who have a specific tag (ID)? (leave blank for all):  """)
        
    if tag_id:
        tag_id = int(tag_id)


    
    #destination_app = ''
    #while len(destination_app) <= 0:
    #    destination_app = 'ap187'#input('Destination Application Name:\n')
    #destination_port = 0
    #while len(str(destination_port)) != 5:
    #    try:
    #        destination_port = 27014#input('Destination Application Port:\n')
    #    except ValueError:
    #        print('Port number must be 5-digits.\n')
    
    
    


    company_ids = None
    contact_ids = None
    job_ids = None
    product_ids = None
    tag_ids = None
    # Instantiate databases
    source = Database(source_app, source_port)

    # Make relationship directory if not exists
    package_dir = os.path.dirname(os.path.abspath(__file__))
    rel_dir = package_dir + f'/app/{source_app}'
    os.makedirs(rel_dir, exist_ok=True)

    start_count_contact = source.get_count(
        'Contact',
        'WHERE IsUser=0 AND Id<>CompanyID'
    )
    start_count_tag = source.get_count('ContactGroup')
    start_count_tag_assign = source.get_count('ContactGroupAssign')
    #start_count_leadsource = source.get_count('LeadSource')
    start_count_company = source.get_count(
        'Contact',
        'WHERE Id=CompanyID'
    )
    start_count_opportunity = source.get_count('Opportunity')
    start_count_product = source.get_count('Product')
    start_count_contact_action = source.get_count('ContactAction')
    start_count_order = source.get_count('Job')
    start_count_subscription = source.get_count('JobRecurring')

    print('App: ' + source_app)
    ##### Contacts
    print('Contacts: ' + str(start_count_contact))
    contact_export = adt.export_contacts(source, tag_id)
    if tag_id:
        contact_ids = contact_export['Id'].tolist()
        company_ids = contact_export['CompanyID'].tolist()
        company_ids = list(set(company_ids))
    print('Exported Contacts: ' + str(len(contact_export)))
    contact_export.to_csv(f'{rel_dir}/Contacts.csv', encoding='utf-8', index=False, header=True)

    ##### Tags Assigned
    print('Tags Assigned: ' + str(start_count_tag_assign))
    tag_applications_export = adt.export_tag_applicaitons(source, contact_ids)
    if tag_id:
        tag_ids = tag_applications_export['GroupId'].tolist()
        tag_ids = list(set(tag_ids))
    print('Exported Tags Assgined: ' + str(len(tag_applications_export)))
    tag_applications_export.to_csv(f'{rel_dir}/Tags_Assigned.csv', encoding='utf-8', index=False, header=True)

    ##### Tags
    print('Tags: ' + str(start_count_tag))
    tags_export = adt.export_tags(source, tag_ids)
    print('Exported Tags: ' + str(len(tags_export)))
    tags_export.to_csv(f'{rel_dir}/Tags.csv', encoding='utf-8', index=False, header=True)

    #print('Leadsources: ' + str(start_count_leadsource))

    ##### Companies
    print('Companies: ' + str(start_count_company))
    companies_export = adt.export_companies(source, company_ids)
    print('Exported Companies: ' + str(len(companies_export)))
    companies_export.to_csv(f'{rel_dir}/Companies.csv', encoding='utf-8', index=False, header=True)

    print('Opportunities: ' + str(start_count_opportunity))
    opportunity_export = adt.export_opportunities(source, contact_ids)
    print('Exported Opportunities: ' + str(len(opportunity_export)))
    opportunity_export.to_csv(f'{rel_dir}/Opportunities.csv', encoding='utf-8', index=False, header=True)

    stage_export = adt.export_stages(source)
    stage_export.to_csv(f'{rel_dir}/Stages.csv', encoding='utf-8', index=False, header=True)

    ##### Contact Actions
    print('Contact Actions: ' + str(start_count_contact_action))
    notes_export = adt.export_notes(source, contact_ids)
    print('Exported Notes: ' + str(len(notes_export)))
    notes_export.to_csv(f'{rel_dir}/Notes.csv', encoding='utf-8', index=False, header=True)
    tasks_export = adt.export_tasks(source, contact_ids)
    print('Exported Tasks: ' + str(len(tasks_export)))
    tasks_export.to_csv(f'{rel_dir}/Tasks.csv', encoding='utf-8', index=False, header=True)
    appointments_export = adt.export_appointments(source, contact_ids)
    print('Exported Appointments: ' + str(len(appointments_export)))
    appointments_export.to_csv(f'{rel_dir}/Appointments.csv', encoding='utf-8', index=False, header=True)

    print('Orders: ' + str(start_count_order))
    order_job_export = adt.export_job(source, contact_ids)
    print('Exported Orders: ' + str(len(order_job_export)))
    if len(order_job_export) != 0:
        if tag_id:
            job_ids = order_job_export['Id'].tolist()
            job_ids = list(set(job_ids))
        order_job_export.to_csv(f'{rel_dir}/OrderNoItems.csv', encoding='utf-8', index=False, header=True)

        order_item_export = adt.export_order_item(source, job_ids)
        if tag_id:
            product_ids = order_item_export['ProductId'].tolist()
        #product_ids = list(set(product_ids))
        print('Exported order items: ' + str(len(order_item_export)))
        order_item_export.to_csv(f'{rel_dir}/OrderItems.csv', encoding='utf-8', index=False, header=True)

        payment_export = adt.export_payment(source, job_ids)
        print('Exported payments: ' + str(len(payment_export)))
        payment_export.to_csv(f'{rel_dir}/Payment.csv', encoding='utf-8', index=False, header=True)

    ##### Subscriptions
    print('Subscriptions: ' + str(start_count_subscription))
    if start_count_subscription != 0:
        subscriptions_export = adt.export_subscriptions(source, contact_ids)
        if tag_id:
            product_ids_from_subs = subscriptions_export['ProductId'].tolist()
            product_ids.extend(product_ids_from_subs)
            product_ids = list(set(product_ids))
        print('Exported Subscriptions: ' + str(len(subscriptions_export)))
        subscriptions_export.to_csv(f'{rel_dir}/Subscriptions.csv', encoding='utf-8', index=False, header=True)

    ##### Products
    print('Products: ' + str(start_count_product))
    product_export = adt.export_products(source, product_ids)
    print('Exported Products: ' + str(len(product_export)))
    product_export.to_csv(f'{rel_dir}/Products.csv', encoding='utf-8', index=False, header=True)


    source.close()

if __name__ == '__main__':
    main()