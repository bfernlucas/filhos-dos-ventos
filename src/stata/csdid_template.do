* Filhos dos Ventos - template de estimacao com csdid

clear all
set more off

* Rode este script a partir da raiz do projeto ou ajuste a linha abaixo.
global PROJROOT "."

cd "$PROJROOT"

import delimited using "data/processed/panel/painel_municipio_ano_2016_2025.csv", clear

destring cd_mun ano, replace force
xtset cd_mun ano

gen gvar = ano_primeira_eolica
replace gvar = 0 if missing(gvar)

gen ever_treated_check = (gvar > 0)
gen treated_it = (eolica_presenca > 0)

* Outcome principal em taxa por mil nascimentos.
* Ajuste a especificacao conforme a estrategia final de amostra.
csdid tx_pais_ausentes_1000 pop_total_2010 share_rural_2010, ///
    ivar(cd_mun) time(ano) gvar(gvar) notyet

estat all
estat event

* Outcome alternativo.
csdid tx_reconhecimento_1000 pop_total_2010 share_rural_2010, ///
    ivar(cd_mun) time(ano) gvar(gvar) notyet

estat all
estat event
