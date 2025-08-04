def ensure_customer_batch_date(customer, batch_date, context):
    import logging
    if 'context' not in customer or customer['context'] is None:
        customer['context'] = dict(context)
    customer['context']['execution_date'] = batch_date
    customer['execution_date'] = batch_date
    logging.info(f"[ensure_customer_batch_date] customer={customer.get('accntid')}, batch_date={batch_date}, context.execution_date={customer['context'].get('execution_date')}, customer.execution_date={customer.get('execution_date')}")

