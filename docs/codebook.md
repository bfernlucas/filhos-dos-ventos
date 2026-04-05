# Codebook

## Painel municipio-ano

Arquivo principal:

- `data/processed/panel/painel_municipio_ano_2016_2025.csv`

Unidade de observacao:

- municipio-ano para os 1.794 municipios do Nordeste entre 2016 e 2025.

## Variaveis principais

- `cd_mun`: identificador municipal IBGE com 7 digitos.
- `nm_mun`: nome do municipio.
- `sigla_uf`: sigla da unidade da federacao.
- `ano`: ano calendario.
- `pais_ausentes`: registros anuais de pais ausentes.
- `reconhecimento_paternidade`: registros anuais de reconhecimento de paternidade.
- `nascimentos`: total anual de nascimentos observado na base de registro civil.
- `tx_pais_ausentes_1000`: pais ausentes por mil nascimentos.
- `tx_reconhecimento_1000`: reconhecimentos por mil nascimentos.
- `semi_dum`: dummy igual a 1 para municipios no semiarido oficial (SUDENE).
- `semi_txt`: rotulo textual (`Sim`/`Nao`) do semiarido oficial SUDENE.
- `eolica_presenca`: dummy anual igual a 1 quando o municipio possui eolica em operacao.
- `eolica_pot_out_kw`: potencia outorgada acumulada no municipio-ano.
- `eolica_pot_fisc_kw`: potencia fiscalizada acumulada no municipio-ano.
- `ano_primeira_eolica`: primeiro ano com eolica no municipio.
- `eolica_pre2016`: dummy igual a 1 se o municipio ja tinha eolica antes de 2016.
- `eolica_pre2016_50km`: dummy igual a 1 se havia eolica pre-2016 em um raio de 50 km.
- `ever_treated`: dummy igual a 1 se o municipio recebe tratamento em algum momento.
- `event_time`: ano relativo ao primeiro tratamento.
- `wind_v100`: velocidade media do vento a 100 metros, agregada ao municipio.
- `pop_total_2010`: populacao total do Censo 2010.
- `share_rural_2010`: participacao da populacao rural no Censo 2010.
- `area_km2`: area municipal em quilometros quadrados.

## Estrategia de pareamento (merge)

- **Censo 2010 -> IBGE 2025**: cruzamento deterministico pelo codigo IBGE
  (`cd_raw` de 6 digitos da planilha .1.1.xls == primeiros 6 digitos do
  `cd_mun` de 7 digitos). Sem perdas por grafia.
- **Registro Civil (ARPEN) -> IBGE 2025**: match exato por nome municipal
  normalizado dentro da UF e, para o residuo, match aproximado via
  RapidFuzz (`WRatio`, limiar 88/100) dentro da mesma UF. Cada par fuzzy
  aceito e registrado no metadata (`diagnosticos_qualidade.merge_reports`)
  para auditoria.
- **Pontos ANEEL e CEPEL -> IBGE 2025**: spatial join por coordenadas
  (within). Nao depende de nome.
- **SUDENE -> IBGE 2025**: join por `cd_mun` (7 digitos). Deterministico.

## Uso econometrico sugerido

Chaves de merge:

- `cd_mun` para integracao com outras bases municipais.
- `cd_mun` e `ano` para analises em painel.

Script de referencia para estimacao:

- `src/stata/csdid_template.do`
