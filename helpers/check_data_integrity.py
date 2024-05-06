import logging


def check_data_integrity(matching_percentage, required_perentage):
    """
    :param matching_percentage: the percentage returned from the audits comparison before and after data enrichment
    :param required_perentage: the business required % of data matching
    """
    if matching_percentage < required_perentage:
        logging.error("DATA INTEGRITY NOT PRESERVER AT REQUIRED LEVEL")
        # Send alert to analysts or people on call


