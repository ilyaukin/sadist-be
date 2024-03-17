import pymongo
from pymongo.database import Database


def upgrade(db: Database):
    db.dl_currency.create_index([("text", pymongo.ASCENDING)])
    db.currency_list.insert_many([
        {'_id': 'AFN', 'name': 'Afghani'},
        {'_id': 'EUR', 'name': 'Euro'},
        {'_id': 'ALL', 'name': 'Lek'},
        {'_id': 'DZD', 'name': 'Algerian Dinar'},
        {'_id': 'USD', 'name': 'US Dollar'},
        {'_id': 'AOA', 'name': 'Kwanza'},
        {'_id': 'XCD', 'name': 'East Caribbean Dollar'},
        {'_id': 'ARS', 'name': 'Argentine Peso'},
        {'_id': 'AMD', 'name': 'Armenian Dram'},
        {'_id': 'AWG', 'name': 'Aruban Florin'},
        {'_id': 'AUD', 'name': 'Australian Dollar'},
        {'_id': 'AZN', 'name': 'Azerbaijan Manat'},
        {'_id': 'BSD', 'name': 'Bahamian Dollar'},
        {'_id': 'BHD', 'name': 'Bahraini Dinar'},
        {'_id': 'BDT', 'name': 'Taka'},
        {'_id': 'BBD', 'name': 'Barbados Dollar'},
        {'_id': 'BYN', 'name': 'Belarusian Ruble'},
        {'_id': 'BZD', 'name': 'Belize Dollar'},
        {'_id': 'XOF', 'name': 'CFA Franc BCEAO'},
        {'_id': 'BMD', 'name': 'Bermudian Dollar'},
        {'_id': 'INR', 'name': 'Indian Rupee'},
        {'_id': 'BTN', 'name': 'Ngultrum'},
        {'_id': 'BOB', 'name': 'Boliviano'},
        {'_id': 'BOV', 'name': 'Mvdol'},
        {'_id': 'BAM', 'name': 'Convertible Mark'},
        {'_id': 'BWP', 'name': 'Pula'},
        {'_id': 'NOK', 'name': 'Norwegian Krone'},
        {'_id': 'BRL', 'name': 'Brazilian Real'},
        {'_id': 'BND', 'name': 'Brunei Dollar'},
        {'_id': 'BGN', 'name': 'Bulgarian Lev'},
        {'_id': 'BIF', 'name': 'Burundi Franc'},
        {'_id': 'CVE', 'name': 'Cabo Verde Escudo'},
        {'_id': 'KHR', 'name': 'Riel'},
        {'_id': 'XAF', 'name': 'CFA Franc BEAC'},
        {'_id': 'CAD', 'name': 'Canadian Dollar'},
        {'_id': 'KYD', 'name': 'Cayman Islands Dollar'},
        {'_id': 'CLP', 'name': 'Chilean Peso'},
        {'_id': 'CLF', 'name': 'Unidad de Fomento'},
        {'_id': 'CNY', 'name': 'Yuan Renminbi'},
        {'_id': 'COP', 'name': 'Colombian Peso'},
        {'_id': 'COU', 'name': 'Unidad de Valor Real'},
        {'_id': 'KMF', 'name': 'Comorian Franc '},
        {'_id': 'CDF', 'name': 'Congolese Franc'},
        {'_id': 'NZD', 'name': 'New Zealand Dollar'},
        {'_id': 'CRC', 'name': 'Costa Rican Colon'},
        {'_id': 'HRK', 'name': 'Kuna'},
        {'_id': 'CUP', 'name': 'Cuban Peso'},
        {'_id': 'CUC', 'name': 'Peso Convertible'},
        {'_id': 'ANG', 'name': 'Netherlands Antillean Guilder'},
        {'_id': 'CZK', 'name': 'Czech Koruna'},
        {'_id': 'DKK', 'name': 'Danish Krone'},
        {'_id': 'DJF', 'name': 'Djibouti Franc'},
        {'_id': 'DOP', 'name': 'Dominican Peso'},
        {'_id': 'EGP', 'name': 'Egyptian Pound'},
        {'_id': 'SVC', 'name': 'El Salvador Colon'},
        {'_id': 'ERN', 'name': 'Nakfa'},
        {'_id': 'SZL', 'name': 'Lilangeni'},
        {'_id': 'ETB', 'name': 'Ethiopian Birr'},
        {'_id': 'FKP', 'name': 'Falkland Islands Pound'},
        {'_id': 'FJD', 'name': 'Fiji Dollar'},
        {'_id': 'XPF', 'name': 'CFP Franc'},
        {'_id': 'GMD', 'name': 'Dalasi'},
        {'_id': 'GEL', 'name': 'Lari'},
        {'_id': 'GHS', 'name': 'Ghana Cedi'},
        {'_id': 'GIP', 'name': 'Gibraltar Pound'},
        {'_id': 'GTQ', 'name': 'Quetzal'},
        {'_id': 'GBP', 'name': 'Pound Sterling'},
        {'_id': 'GNF', 'name': 'Guinean Franc'},
        {'_id': 'GYD', 'name': 'Guyana Dollar'},
        {'_id': 'HTG', 'name': 'Gourde'},
        {'_id': 'HNL', 'name': 'Lempira'},
        {'_id': 'HKD', 'name': 'Hong Kong Dollar'},
        {'_id': 'HUF', 'name': 'Forint'},
        {'_id': 'ISK', 'name': 'Iceland Krona'},
        {'_id': 'IDR', 'name': 'Rupiah'},
        {'_id': 'XDR', 'name': 'SDR (Special Drawing Right)'},
        {'_id': 'IRR', 'name': 'Iranian Rial'},
        {'_id': 'IQD', 'name': 'Iraqi Dinar'},
        {'_id': 'ILS', 'name': 'New Israeli Sheqel'},
        {'_id': 'JMD', 'name': 'Jamaican Dollar'},
        {'_id': 'JPY', 'name': 'Yen'},
        {'_id': 'JOD', 'name': 'Jordanian Dinar'},
        {'_id': 'KZT', 'name': 'Tenge'},
        {'_id': 'KES', 'name': 'Kenyan Shilling'},
        {'_id': 'KPW', 'name': 'North Korean Won'},
        {'_id': 'KRW', 'name': 'Won'},
        {'_id': 'KWD', 'name': 'Kuwaiti Dinar'},
        {'_id': 'KGS', 'name': 'Som'},
        {'_id': 'LAK', 'name': 'Lao Kip'},
        {'_id': 'LBP', 'name': 'Lebanese Pound'},
        {'_id': 'LSL', 'name': 'Loti'},
        {'_id': 'ZAR', 'name': 'Rand'},
        {'_id': 'LRD', 'name': 'Liberian Dollar'},
        {'_id': 'LYD', 'name': 'Libyan Dinar'},
        {'_id': 'CHF', 'name': 'Swiss Franc'},
        {'_id': 'MOP', 'name': 'Pataca'},
        {'_id': 'MKD', 'name': 'Denar'},
        {'_id': 'MGA', 'name': 'Malagasy Ariary'},
        {'_id': 'MWK', 'name': 'Malawi Kwacha'},
        {'_id': 'MYR', 'name': 'Malaysian Ringgit'},
        {'_id': 'MVR', 'name': 'Rufiyaa'},
        {'_id': 'MRU', 'name': 'Ouguiya'},
        {'_id': 'MUR', 'name': 'Mauritius Rupee'},
        {'_id': 'XUA', 'name': 'ADB Unit of Account'},
        {'_id': 'MXN', 'name': 'Mexican Peso'},
        {'_id': 'MXV', 'name': 'Mexican Unidad de Inversion (UDI)'},
        {'_id': 'MDL', 'name': 'Moldovan Leu'},
        {'_id': 'MNT', 'name': 'Tugrik'},
        {'_id': 'MAD', 'name': 'Moroccan Dirham'},
        {'_id': 'MZN', 'name': 'Mozambique Metical'},
        {'_id': 'MMK', 'name': 'Kyat'},
        {'_id': 'NAD', 'name': 'Namibia Dollar'},
        {'_id': 'NPR', 'name': 'Nepalese Rupee'},
        {'_id': 'NIO', 'name': 'Cordoba Oro'},
        {'_id': 'NGN', 'name': 'Naira'},
        {'_id': 'OMR', 'name': 'Rial Omani'},
        {'_id': 'PKR', 'name': 'Pakistan Rupee'},
        {'_id': 'PAB', 'name': 'Balboa'},
        {'_id': 'PGK', 'name': 'Kina'},
        {'_id': 'PYG', 'name': 'Guarani'},
        {'_id': 'PEN', 'name': 'Sol'},
        {'_id': 'PHP', 'name': 'Philippine Peso'},
        {'_id': 'PLN', 'name': 'Zloty'},
        {'_id': 'QAR', 'name': 'Qatari Rial'},
        {'_id': 'RON', 'name': 'Romanian Leu'},
        {'_id': 'RUB', 'name': 'Russian Ruble'},
        {'_id': 'RWF', 'name': 'Rwanda Franc'},
        {'_id': 'SHP', 'name': 'Saint Helena Pound'},
        {'_id': 'WST', 'name': 'Tala'},
        {'_id': 'STN', 'name': 'Dobra'},
        {'_id': 'SAR', 'name': 'Saudi Riyal'},
        {'_id': 'RSD', 'name': 'Serbian Dinar'},
        {'_id': 'SCR', 'name': 'Seychelles Rupee'},
        {'_id': 'SLL', 'name': 'Leone'},
        {'_id': 'SGD', 'name': 'Singapore Dollar'},
        {'_id': 'XSU', 'name': 'Sucre'},
        {'_id': 'SBD', 'name': 'Solomon Islands Dollar'},
        {'_id': 'SOS', 'name': 'Somali Shilling'},
        {'_id': 'SSP', 'name': 'South Sudanese Pound'},
        {'_id': 'LKR', 'name': 'Sri Lanka Rupee'},
        {'_id': 'SDG', 'name': 'Sudanese Pound'},
        {'_id': 'SRD', 'name': 'Surinam Dollar'},
        {'_id': 'SEK', 'name': 'Swedish Krona'},
        {'_id': 'CHE', 'name': 'WIR Euro'},
        {'_id': 'CHW', 'name': 'WIR Franc'},
        {'_id': 'SYP', 'name': 'Syrian Pound'},
        {'_id': 'TWD', 'name': 'New Taiwan Dollar'},
        {'_id': 'TJS', 'name': 'Somoni'},
        {'_id': 'TZS', 'name': 'Tanzanian Shilling'},
        {'_id': 'THB', 'name': 'Baht'},
        {'_id': 'TOP', 'name': "Pa'anga"},
        {'_id': 'TTD', 'name': 'Trinidad and Tobago Dollar'},
        {'_id': 'TND', 'name': 'Tunisian Dinar'},
        {'_id': 'TRY', 'name': 'Turkish Lira'},
        {'_id': 'TMT', 'name': 'Turkmenistan New Manat'},
        {'_id': 'UGX', 'name': 'Uganda Shilling'},
        {'_id': 'UAH', 'name': 'Hryvnia'},
        {'_id': 'AED', 'name': 'UAE Dirham'},
        {'_id': 'USN', 'name': 'US Dollar (Next day)'},
        {'_id': 'UYU', 'name': 'Peso Uruguayo'},
        {'_id': 'UYI', 'name': 'Uruguay Peso en Unidades Indexadas (UI)'},
        {'_id': 'UYW', 'name': 'Unidad Previsional'},
        {'_id': 'UZS', 'name': 'Uzbekistan Sum'},
        {'_id': 'VUV', 'name': 'Vatu'},
        {'_id': 'VES', 'name': 'Bolívar Soberano'},
        {'_id': 'VND', 'name': 'Dong'},
        {'_id': 'YER', 'name': 'Yemeni Rial'},
        {'_id': 'ZMW', 'name': 'Zambian Kwacha'},
        {'_id': 'ZWL', 'name': 'Zimbabwe Dollar'},
        {'_id': 'XBA', 'name': 'Bond Markets Unit European Composite Unit (EURCO)'},
        {'_id': 'XBB', 'name': 'Bond Markets Unit European Monetary Unit (E.M.U.-6)'},
        {'_id': 'XBC', 'name': 'Bond Markets Unit European Unit of Account 9 (E.U.A.-9)'},
        {'_id': 'XBD', 'name': 'Bond Markets Unit European Unit of Account 17 (E.U.A.-17)'},
        {'_id': 'XTS', 'name': 'Codes specifically reserved for testing purposes'},
        {'_id': 'XXX', 'name': 'The codes assigned for transactions where no currency is involved'},
        {'_id': 'XAU', 'name': 'Gold'},
        {'_id': 'XPD', 'name': 'Palladium'},
        {'_id': 'XPT', 'name': 'Platinum'},
        {'_id': 'XAG', 'name': 'Silver'},
    ])


def downgrade(db: Database):
    pass