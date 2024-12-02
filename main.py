import logging
from analyses.flash_loan_frequency import analyze_flash_loan_frequency, extract_day_hour
from analyses.flash_loan_fee import analyze_flash_loan_fee
from analyses.flash_loan_volume import analyze_flash_loan_volume, analyze_flash_loan_volume_all
from analyses.flash_loan_tokens import analyze_flash_loan_tokens
from analyses.transaction_sequence import analyze_flash_loan_wallets
from utils.decoder_input import decode_flash_loan_transaction
from data.data_loader import create_indexes
from utils.helpers import save_to_cache
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    logging.info("Iniciando a análise de dados DeFi.")
    #create_indexes()

    #Executar a análise das taxas de flash loans
    logging.info("Analisando taxas de flash loans...")
    ethereum_metrics, polygon_metrics = analyze_flash_loan_fee()

    #Exibir os resultados
    print("Métricas Ethereum:", ethereum_metrics)
    print("Métricas Polygon:", polygon_metrics)

    logging.info("Analisando a frequência de flash loans...")
    extract_day_hour()

    logging.info("Analisando volume de flash loans...")
    volume_data = analyze_flash_loan_volume_all()
    save_to_cache('flash_loan_volume_all', volume_data)

    #logging.info("Análise concluída com sucesso.")
    #print(volume_data)


    flash_loan_wallets_analysis_polygon, flash_loan_wallets_analysis_ethereum = analyze_flash_loan_wallets()

    save_to_cache('flash_loan_wallets_analysis_polygon',
                  json.dumps(flash_loan_wallets_analysis_polygon.to_dict(orient='records')))
    save_to_cache('flash_loan_wallets_analysis_ethereum',
                  json.dumps(flash_loan_wallets_analysis_ethereum.to_dict(orient='records')))

    logging.info("Dados salvos no cache Redis.")


#    logging.info("Analisando taxas de flash loans...")
#    analyze_flash_loan_fee()

    logging.info("Analisando distribuição de tokens...")
    analyze_flash_loan_tokens()

#    logging.info("Analisando sequência de transações...")
#    analyze_transaction_sequence()

    logging.info("Análise concluída com sucesso.")

#    analyze_and_cache_random_transactions()




if __name__ == "__main__":
    main()