import mariadb
import sys
import time
from fastapi import FastAPI
from enum import Enum
from starlette.responses import JSONResponse

app = FastAPI()


class CountName(str, Enum):
    countALL = "all"
    countBLDT = "bldt"
    countBRST = "brst"
    countCLRC = "clrc"
    countGSTR = "gstr"
    countKDNY = "kdny"
    countLUNG = "lung"
    countLVER = "lver"
    countOVRY = "ovry"
    countPNCT = "pnct"
    countPRST = "prst"
    countDG = "dg"
    countOPRT = "oprt"
    countFMHT = "fmht"
    countHLNF = "hlnf"
    countCASB = "casb"
    countRD = "rd"
    countBLDTalive = "bldt-alive"
    countBRSTalive = "brst-alive"
    countCLRCalive = "clrc-alive"
    countGSTRalive = "gstr-alive"
    countKDNYalive = "kdny-alive"
    countLUNGalive = "lung-alive"
    countLVERalive = "lver-alive"
    countOVRYalive = "ovry-alive"
    countPNCTalive = "pnct-alive"
    countPRSTalive = "prst-alive"


try:
    conn = mariadb.connect(
        user='inviz_cancer',
        password='cancer12#$',
        host='192.168.0.78',
        port=3306,
        database="cancerDB"
    )
    print("DB connected")

except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

# 시작 시간
start = time.time()

# Queries
select_each = "SELECT a.cnt, b.cnt, c.cnt, d.cnt, e.cnt, f.cnt, g.cnt, h.cnt, i.cnt, j.cnt FROM"
select_plus = "SELECT a.cnt + b.cnt + c.cnt + d.cnt + e.cnt " \
              "+ f.cnt + g.cnt + h.cnt + i.cnt + j.cnt as count FROM"

# 암종 별 환자 수
query = " (SELECT COUNT(PT_SBST_NO) as cnt FROM {c}_PT_BSNF)"
cur.execute(select_each +
            query.format(c="BLDT") + "a," +
            query.format(c="BRST") + "b," +
            query.format(c="CLRC") + "c," +
            query.format(c="GSTR") + "d," +
            query.format(c="KDNY") + "e," +
            query.format(c="LUNG") + "f," +
            query.format(c="LVER") + "g," +
            query.format(c="OVRY") + "h," +
            query.format(c="PNCT") + "i," +
            query.format(c="PRST") + "j")
getFetchCount = cur.fetchone()
countBLDT = getFetchCount[0]
countBRST = getFetchCount[1]
countCLRC = getFetchCount[2]
countGSTR = getFetchCount[3]
countKDNY = getFetchCount[4]
countLUNG = getFetchCount[5]
countLVER = getFetchCount[6]
countOVRY = getFetchCount[7]
countPNCT = getFetchCount[8]
countPRST = getFetchCount[9]

# 전체 환자 수
countALL = countBLDT + countBRST + countCLRC + countGSTR + countKDNY \
           + countLUNG + countLVER + countOVRY + countPNCT + countPRST

print("암종별 : ", countBLDT, countBRST, countCLRC, countGSTR, countKDNY,
      countLUNG, countLVER, countOVRY, countPNCT, countPRST)
print("환자수 : ", countALL)


# 진단 건수
query = " (select count(*) as cnt from {c}_DG_THNF)"
cur.execute(select_plus +
            query.format(c="BLDT") + "a," +
            query.format(c="BRST") + "b," +
            query.format(c="CLRC") + "c," +
            query.format(c="GSTR") + "d," +
            query.format(c="KDNY") + "e," +
            query.format(c="LUNG") + "f," +
            query.format(c="LVER") + "g," +
            query.format(c="OVRY") + "h," +
            query.format(c="PNCT") + "i," +
            query.format(c="PRST") + "j")
countDG = cur.fetchone()[0]

print("진단수 : ", countDG)

# 암 수술 환자 수
query = " (select count(DISTINCT PT_SBST_NO) as cnt from {c}_OPRT_NFRM)"
cur.execute(select_plus +
            query.format(c="BLDT") + "a," +
            query.format(c="BRST") + "b," +
            query.format(c="CLRC") + "c," +
            query.format(c="GSTR") + "d," +
            query.format(c="KDNY") + "e," +
            query.format(c="LUNG") + "f," +
            query.format(c="LVER") + "g," +
            query.format(c="OVRY") + "h," +
            query.format(c="PNCT") + "i," +
            query.format(c="PRST") + "j")
countOPRT = cur.fetchone()[0]

print("수술수 : ", countOPRT)
print("수술률 : ", round(countOPRT/countALL, 1))

# 가족력 환자 수
query = " (select count(DISTINCT PT_SBST_NO) as cnt from {c}_PT_FMHT)"
cur.execute(select_plus +
            query.format(c="BLDT") + "a," +
            query.format(c="BRST") + "b," +
            query.format(c="CLRC") + "c," +
            query.format(c="GSTR") + "d," +
            query.format(c="KDNY") + "e," +
            query.format(c="LUNG") + "f," +
            query.format(c="LVER") + "g," +
            query.format(c="OVRY") + "h," +
            query.format(c="PNCT") + "i," +
            query.format(c="PRST") + "j")
countFMHT = cur.fetchone()[0]

print("가족력 : ", countFMHT)

# 외래 암 환자 / 입원 암 환자
query = " (select count(DISTINCT PT_SBST_NO) as cnt from {c}_PT_HLNF)"
cur.execute(select_plus +
            query.format(c="BLDT") + "a," +
            query.format(c="BRST") + "b," +
            query.format(c="CLRC") + "c," +
            query.format(c="GSTR") + "d," +
            query.format(c="KDNY") + "e," +
            query.format(c="LUNG") + "f," +
            query.format(c="LVER") + "g," +
            query.format(c="OVRY") + "h," +
            query.format(c="PNCT") + "i," +
            query.format(c="PRST") + "j")
countHLNF = cur.fetchone()[0]

print("외래암 : ", countALL - countHLNF)
print("임원암 : ", countHLNF)

# 치료 건수
print("치료수")

# 항암제 치료
query = " (select count(*) as cnt from {c}_TRTM_CASB)"
cur.execute(select_plus +
            query.format(c="BLDT") + "a," +
            query.format(c="BRST") + "b," +
            query.format(c="CLRC") + "c," +
            query.format(c="GSTR") + "d," +
            query.format(c="KDNY") + "e," +
            query.format(c="LUNG") + "f," +
            query.format(c="LVER") + "g," +
            query.format(c="OVRY") + "h," +
            query.format(c="PNCT") + "i," +
            query.format(c="PRST") + "j")
countCASB = cur.fetchone()[0]

print("항암제 : ", countCASB)

# 방사선 치료
query = " (select count(*) as cnt from {c}_TRTM_RD)"
cur.execute(select_plus +
            query.format(c="BLDT") + "a," +
            query.format(c="BRST") + "b," +
            query.format(c="CLRC") + "c," +
            query.format(c="GSTR") + "d," +
            query.format(c="KDNY") + "e," +
            query.format(c="LUNG") + "f," +
            query.format(c="LVER") + "g," +
            query.format(c="OVRY") + "h," +
            query.format(c="PNCT") + "i," +
            query.format(c="PRST") + "j")
countRD = cur.fetchone()[0]

print("방사선 : ", countRD)


# 11종 암종별 환자 5년 생존율
query = " (SELECT COUNT(*) as cnt FROM {c}_PT_BSNF JOIN {c}_DEAD_NFRM USING(PT_SBST_NO) " \
        "WHERE DATEDIFF({c}_DEAD_NFRM.DEAD_YMD, {c}_PT_BSNF.BSPT_FRST_DIAG_YMD) > 5)"
query2 = " (SELECT COUNT(*) as cnt FROM {c}_PT_BSNF JOIN {c}_DEAD_NFRM USING(PT_SBST_NO) " \
         "WHERE DATEDIFF({c}_DEAD_NFRM.DEATH_YMD, {c}_PT_BSNF.FRMD_YMD) > 5)"

cur.execute(select_each +
            query.format(c="BLDT") + "a," +
            query2.format(c="BRST") + "b," +
            query.format(c="CLRC") + "c," +
            query.format(c="GSTR") + "d," +
            query.format(c="KDNY") + "e," +
            query.format(c="LUNG") + "f," +
            query.format(c="LVER") + "g," +
            query2.format(c="OVRY") + "h," +
            query.format(c="PNCT") + "i," +
            query.format(c="PRST") + "j"
            )
getFetchAlive = cur.fetchone()
countBLDTalive = getFetchAlive[0]
countBRSTalive = getFetchAlive[1]
countCLRCalive = getFetchAlive[2]
countGSTRalive = getFetchAlive[3]
countKDNYalive = getFetchAlive[4]
countLUNGalive = getFetchAlive[5]
countLVERalive = getFetchAlive[6]
countOVRYalive = getFetchAlive[7]
countPNCTalive = getFetchAlive[8]
countPRSTalive = getFetchAlive[9]

print("담도암")
print("생존수 : ", countBLDTalive)
print("생존율 : ", round(countBLDTalive/countBLDT, 3))
print("유방암")
print("생존수 : ", countBRSTalive)
print("생존율 : ", round(countBRSTalive/countBRST, 3))
print("대장암")
print("생존수 : ", countCLRCalive)
print("생존율 : ", round(countCLRCalive/countCLRC, 3))
print("위암")
print("생존수 : ", countGSTRalive)
print("생존율 : ", round(countGSTRalive/countGSTR, 3))
print("신장암")
print("생존수 : ", countKDNYalive)
print("생존율 : ", round(countKDNYalive/countKDNY, 3))
print("폐암")
print("생존수 : ", countLUNGalive)
print("생존율 : ", round(countLUNGalive/countLUNG, 3))
print("간암")
print("생존수 : ", countLVERalive)
print("생존율 : ", round(countLVERalive/countLVER, 3))
print("난소암")
print("생존수 : ", countOVRYalive)
print("생존율 : ", round(countOVRYalive/countOVRY, 3))
print("췌장암")
print("생존수 : ", countPNCTalive)
print("생존율 : ", round(countPNCTalive/countPNCT, 3))
print("전립선암")
print("생존수 : ", countPRSTalive)
print("생존율 : ", round(countPRSTalive/countPRST, 3))


@app.get("/counting/{count_what}")
def counting(count_what: CountName):
    if count_what.value == "all":
        return JSONResponse({'count_all': countALL})
    if count_what.value == "bldt":
        return JSONResponse({'count_bldt': countBLDT})
    if count_what.value == "brst":
        return JSONResponse({'count_brst': countBRST})
    if count_what.value == "clrc":
        return JSONResponse({'count_clrc': countCLRC})
    if count_what.value == "gstr":
        return JSONResponse({'count_gstr': countGSTR})
    if count_what.value == "kdny":
        return JSONResponse({'count_kdny': countKDNY})
    if count_what.value == "lung":
        return JSONResponse({'count_lung': countLUNG})
    if count_what.value == "lver":
        return JSONResponse({'count_lver': countLVER})
    if count_what.value == "ovry":
        return JSONResponse({'count_ovry': countOVRY})
    if count_what.value == "pnct":
        return JSONResponse({'count_pnct': countPNCT})
    if count_what.value == "prst":
        return JSONResponse({'count_prst': countPRST})
    if count_what.value == "dg":
        return JSONResponse({'count_dg': countDG})
    if count_what.value == "oprt":
        return JSONResponse({'count_oprt': countOPRT})
    if count_what.value == "fmht":
        return JSONResponse({'count_fmht': countFMHT})
    if count_what.value == "hlnf":
        return JSONResponse({'count_hlnf': countHLNF})
    if count_what.value == "casb":
        return JSONResponse({'count_casb': countCASB})
    if count_what.value == "rd":
        return JSONResponse({'count_rd': countRD})
    if count_what.value == "bldt-alive":
        return JSONResponse({'count_bldt_alive': countBLDTalive,
                             'ratio_bldt_alive': round(countBLDTalive/countALL, 4) * 1000})
    if count_what.value == "brst-alive":
        return JSONResponse({'count_brst_alive': countBRSTalive,
                             'ratio_brst_alive': round(countBRSTalive/countALL, 4) * 1000})
    if count_what.value == "clrc-alive":
        return JSONResponse({'count_clrc_alive': countCLRCalive,
                             'ratio_clrc_alive': round(countCLRCalive/countALL, 4) * 1000})
    if count_what.value == "gstr-alive":
        return JSONResponse({'count_gstr_alive': countGSTRalive,
                             'ratio_gstr_alive': round(countGSTRalive/countALL, 4) * 1000})
    if count_what.value == "kdny-alive":
        return JSONResponse({'count_kdny_alive': countKDNYalive,
                             'ratio_kdny_alive': round(countKDNYalive/countALL, 4) * 1000})
    if count_what.value == "lung-alive":
        return JSONResponse({'count_lung_alive': countLUNGalive,
                             'ratio_lung_alive': round(countLUNGalive/countALL, 4) * 1000})
    if count_what.value == "lver-alive":
        return JSONResponse({'count_lver_alive': countLVERalive,
                             'ratio_lver_alive': round(countLVERalive/countALL, 4) * 1000})
    if count_what.value == "ovry-alive":
        return JSONResponse({'count_ovry_alive': countOVRYalive,
                             'ratio_ovry_alive': round(countOVRYalive/countALL, 4) * 1000})
    if count_what.value == "pnct-alive":
        return JSONResponse({'count_pnct_alive': countPNCTalive,
                             'ratio_pnct_alive': round(countPNCTalive/countALL, 4) * 1000})
    if count_what.value == "prst-alive":
        return JSONResponse({'count_prst_alive': countPRSTalive,
                             'ratio_prst_alive': round(countPRSTalive/countALL, 4) * 1000})


# 종료 시간
end = time.time()

print("총시간 : ", round(end - start, 3))