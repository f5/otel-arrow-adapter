# Benchmark results

3 Protocols + data encoding: 
- OTLP ZSTD: Standard OTLP protocol with ZSTD compression
- OTLP DICT ZSTD: OTLP protocol with dictionary compression, and ZSTD compression
- OTel ARROW ZSTD: OTel Arrow protocol with ZSTD compression

2 modes (used by OTLP DICT and OTel ARROW):
- STREAM: gRPC streaming mode
- UNARY RPC: gRPC unary RPC mode

## Hipster Shop Metrics

| PROTO MSG SIZE           | OTLP ZSTD - MEAN        | OTLP DICT ZSTD+STREAM MODE - MEAN | OTel ARROW ZSTD+STREAM MODE - MEAN | OTLP DICT ZSTD+UNARY RPC - MEAN  | OTel ARROW ZSTD+UNARY RPC MODE - MEAN |
|--------------------------|-------------------------|-----------------------------------|------------------------------------|----------------------------------|---------------------------------------|
| **Uncompressed (bytes)** |                         |                                   |                                    |                                  |                                       |  
| batch_size: 10           | 5196  (total: 23 MB)    | 1552 (x  3.35) (total: 6.8 MB)    | 3129 (x  1.66) (total: 14 MB)      | 2646 (x  1.96) (total: 12 MB)    | 8205 (x  0.63) (total: 36 MB)         |
| batch_size: 100          | 48498  (total: 21 MB)   | 14458 (x  3.35) (total: 6.3 MB)   | 15100 (x  3.21) (total: 6.6 MB)    | 17997 (x  2.69) (total: 7.8 MB)  | 23070 (x  2.10) (total: 10 MB)        |
| batch_size: 500          | 239192  (total: 21 MB)  | 70999 (x  3.37) (total: 6.2 MB)   | 67595 (x  3.54) (total: 5.9 MB)    | 81326 (x  2.94) (total: 7.1 MB)  | 81303 (x  2.94) (total: 7.1 MB)       |
| batch_size: 1000         | 478625  (total: 21 MB)  | 142142 (x  3.37) (total: 6.1 MB)  | 133896 (x  3.57) (total: 5.8 MB)   | 156787 (x  3.05) (total: 6.7 MB) | 154857 (x  3.09) (total: 6.7 MB)      |
| batch_size: 2000         | 952550  (total: 20 MB)  | 282574 (x  3.37) (total: 5.9 MB)  | 263606 (x  3.61) (total: 5.5 MB)   | 300070 (x  3.17) (total: 6.3 MB) | 292538 (x  3.26) (total: 6.1 MB)      |
| batch_size: 4000         | 1913163  (total: 19 MB) | 567416 (x  3.37) (total: 5.7 MB)  | 520595 (x  3.67) (total: 5.2 MB)   | 585788 (x  3.27) (total: 5.9 MB) | 549615 (x  3.48) (total: 5.5 MB)      |
| batch_size: 5000         | 2321630  (total: 19 MB) | 689017 (x  3.37) (total: 5.5 MB)  | 630868 (x  3.68) (total: 5.0 MB)   | 707830 (x  3.28) (total: 5.7 MB) | 659884 (x  3.52) (total: 5.3 MB)      |
| **Compressed (bytes)**   |                         |                                   |                                    |                                  |                                       |
| batch_size: 10           | 988  (total: 4.3 MB)    | 390 (x  2.53) (total: 1.7 MB)     | 610 (x  1.62) (total: 2.7 MB)      | 1005 (x  0.98) (total: 4.4 MB)   | 2294 (x  0.43) (total: 10 MB)         |
| batch_size: 100          | 3784  (total: 1.6 MB)   | 1746 (x  2.17) (total: 760 kB)    | 1731 (x  2.19) (total: 753 kB)     | 3509 (x  1.08) (total: 1.5 MB)   | 4997 (x  0.76) (total: 2.2 MB)        |
| batch_size: 500          | 13857  (total: 1.2 MB)  | 7180 (x  1.93) (total: 625 kB)    | 5663 (x  2.45) (total: 493 kB)     | 12196 (x  1.14) (total: 1.1 MB)  | 13272 (x  1.04) (total: 1.2 MB)       |
| batch_size: 1000         | 24346  (total: 1.0 MB)  | 13592 (x  1.79) (total: 584 kB)   | 10088 (x  2.41) (total: 434 kB)    | 20206 (x  1.20) (total: 869 kB)  | 20567 (x  1.18) (total: 884 kB)       |
| batch_size: 2000         | 39669  (total: 833 kB)  | 24193 (x  1.64) (total: 508 kB)   | 17238 (x  2.30) (total: 362 kB)    | 31729 (x  1.25) (total: 666 kB)  | 29389 (x  1.35) (total: 617 kB)       |
| batch_size: 4000         | 64622  (total: 646 kB)  | 43162 (x  1.50) (total: 432 kB)   | 27991 (x  2.31) (total: 280 kB)    | 50749 (x  1.27) (total: 508 kB)  | 40270 (x  1.60) (total: 403 kB)       |
| batch_size: 5000         | 75325  (total: 603 kB)  | 50870 (x  1.48) (total: 407 kB)   | 32800 (x  2.30) (total: 262 kB)    | 58869 (x  1.28) (total: 471 kB)  | 44532 (x  1.69) (total: 356 kB)       |

## Multivariate Metrics

| PROTO MSG SIZE           | OTLP ZSTD - MEAN          | OTLP DICT ZSTD+STREAM MODE - MEAN  | OTel ARROW ZSTD+STREAM MODE - MEAN | OTLP DICT ZSTD+UNARY RPC - MEAN    | OTel ARROW ZSTD+UNARY RPC MODE - MEAN |
|--------------------------|---------------------------|------------------------------------|------------------------------------|------------------------------------|---------------------------------------|
| **Uncompressed (bytes)** |                           |                                    |                                    |                                    |                                       |
| batch_size: 10           |    28394  (total: 28 MB)  |     8355 (x  3.40) (total: 8.3 MB) | 8833 (x  3.21) (total: 8.8 MB)     |     8773 (x  3.24) (total: 8.8 MB) | 13550 (x  2.10) (total: 14 MB)        |
| batch_size: 100          |   283402  (total: 28 MB)  |    82944 (x  3.42) (total: 8.2 MB) | 73073 (x  3.88) (total: 7.2 MB)    |    82658 (x  3.43) (total: 8.2 MB) | 79175 (x  3.58) (total: 7.8 MB)       |
| batch_size: 500          |  1416493  (total: 27 MB)  |   414789 (x  3.41) (total: 7.9 MB) | 398907 (x  3.55) (total: 7.6 MB)   |   407355 (x  3.48) (total: 7.7 MB) | 365641 (x  3.87) (total: 6.9 MB)      |
| batch_size: 1000         |  2832178  (total: 26 MB)  |   830519 (x  3.41) (total: 7.5 MB) | 796068 (x  3.56) (total: 7.2 MB)   |   815787 (x  3.47) (total: 7.3 MB) | 803355 (x  3.53) (total: 7.2 MB)      |
| batch_size: 2000         |  5660675  (total: 23 MB)  |  1665684 (x  3.40) (total: 6.7 MB) | 1590630 (x  3.56) (total: 6.4 MB)  |  1637336 (x  3.46) (total: 6.5 MB) | 1598788 (x  3.54) (total: 6.4 MB)     |
| batch_size: 4000         |  8486562  (total: 17 MB)  |  2502719 (x  3.39) (total: 5.0 MB) | 2385053 (x  3.56) (total: 4.8 MB)  |  2460787 (x  3.45) (total: 4.9 MB) | 2393501 (x  3.55) (total: 4.8 MB)     |
| batch_size: 5000         | 14142283  (total: 14 MB)  |  4171314 (x  3.39) (total: 4.2 MB) | 3973999 (x  3.56) (total: 4.0 MB)  |  4117472 (x  3.43) (total: 4.1 MB) | 3982535 (x  3.55) (total: 4.0 MB)     |
| **Compressed (bytes)**   |                           |                                    |                                    |                                    |                                       |
| batch_size: 10           |     2016  (total: 2.0 MB) |     1334 (x  1.51) (total: 1.3 MB) | 776 (x  2.60) (total: 776 kB)      |     1531 (x  1.32) (total: 1.5 MB) | 2124 (x  0.95) (total: 2.1 MB)        |
| batch_size: 100          |    16183  (total: 1.6 MB) |    11185 (x  1.45) (total: 1.1 MB) | 3176 (x  5.09) (total: 314 kB)     |    10624 (x  1.52) (total: 1.1 MB) | 5228 (x  3.10) (total: 518 kB)        |
| batch_size: 500          |    79164  (total: 1.5 MB) |    55371 (x  1.43) (total: 1.1 MB) | 13215 (x  5.99) (total: 251 kB)    |    50053 (x  1.58) (total: 951 kB) | 15322 (x  5.17) (total: 291 kB)       |
| batch_size: 1000         |   158052  (total: 1.4 MB) |   111224 (x  1.42) (total: 1.0 MB) | 24891 (x  6.35) (total: 224 kB)    |   100951 (x  1.57) (total: 909 kB) | 27730 (x  5.70) (total: 250 kB)       |
| batch_size: 2000         |   315951  (total: 1.3 MB) |   225481 (x  1.40) (total: 902 kB) | 47008 (x  6.72) (total: 188 kB)    |   206205 (x  1.53) (total: 825 kB) | 50929 (x  6.20) (total: 204 kB)       |
| batch_size: 4000         |   474969  (total: 950 kB) |   338184 (x  1.40) (total: 676 kB) | 65820 (x  7.22) (total: 132 kB)    |   312577 (x  1.52) (total: 625 kB) | 68445 (x  6.94) (total: 137 kB)       |
| batch_size: 5000         |   792803  (total: 793 kB) |   560934 (x  1.41) (total: 561 kB) | 99458 (x  7.97) (total: 100 kB)    |   525034 (x  1.51) (total: 525 kB) | 102932 (x  7.70) (total: 103 kB)      |

## Logs

| PROTO MSG SIZE           | OTLP ZSTD - MEAN         | OTel ARROW ZSTD+STREAM MODE - MEAN | OTel ARROW ZSTD+UNARY RPC MODE - MEAN |
|--------------------------|--------------------------|------------------------------------|---------------------------------------|
| **Uncompressed (bytes)** |                          |                                    |                                       |
| batch_size: 5000         | 1324852  (total: 130 MB) | 719397 (x  1.84) (total: 70 MB)    | 861324 (x  1.54) (total: 84 MB)       |
| batch_size: 10000        | 2649728  (total: 127 MB) | 1432833 (x  1.85) (total: 69 MB)   | 1714589 (x  1.55) (total: 82 MB)      |
| **Compressed (bytes)**   |                          |                                    |                                       |
| batch_size: 5000         | 263515  (total: 26 MB)   | 130915 (x  2.01) (total: 13 MB)    | 185619 (x  1.42) (total: 18 MB)       |
| batch_size: 10000        | 522715  (total: 25 MB)   | 255211 (x  2.05) (total: 12 MB)    | 364671 (x  1.43) (total: 18 MB)       |

## Hipster Shop Traces

| PROTO MSG SIZE           | OTLP ZSTD - MEAN          | OTLP DICT ZSTD+STREAM MODE - MEAN | OTel ARROW ZSTD+STREAM MODE - MEAN  | OTLP DICT ZSTD+UNARY RPC - MEAN   | OTel ARROW ZSTD+UNARY RPC MODE - MEAN   |
|--------------------------|---------------------------|-----------------------------------|-------------------------------------|-----------------------------------|-----------------------------------------|
| **Uncompressed (bytes)** |                           |                                   |                                     |                                   |                                         |
| batch_size: 500          |   134629  (total: 23 MB)  | 56243 (x  2.39) (total: 9.7 MB)   | 50872 (x  2.65) (total: 8.8 MB)     | 60467 (x  2.23) (total: 10 MB)    | 61539 (x  2.19) (total: 11 MB)          |
| batch_size: 5000         |  1252139  (total: 20 MB)  | 532425 (x  2.35) (total: 8.5 MB)  | 473748 (x  2.64) (total: 7.6 MB)    | 538216 (x  2.33) (total: 8.6 MB)  | 495640 (x  2.53) (total: 7.9 MB)        |
| batch_size: 10000        |  2494746  (total: 18 MB)  | 1063158 (x  2.35) (total: 7.4 MB) | 947829 (x  2.63) (total: 6.6 MB)    | 1066571 (x  2.34) (total: 7.5 MB) | 973397 (x  2.56) (total: 6.8 MB)        |
| **Compressed (bytes)**   |                           |                                   |                                     |                                   |                                         |
| batch_size: 500          |    21662  (total: 3.7 MB) | 17331 (x  1.25) (total: 3.0 MB)   | 12291 (x  1.76) (total: 2.1 MB)     | 20088 (x  1.08) (total: 3.5 MB)   | 17042 (x  1.27) (total: 2.9 MB)         |
| batch_size: 5000         |   178704  (total: 2.9 MB) | 159338 (x  1.12) (total: 2.5 MB)  | 108570 (x  1.65) (total: 1.7 MB)    | 163787 (x  1.09) (total: 2.6 MB)  | 115699 (x  1.54) (total: 1.9 MB)        |
| batch_size: 10000        |   355126  (total: 2.5 MB) | 322501 (x  1.10) (total: 2.3 MB)  | 217190 (x  1.64) (total: 1.5 MB)    | 326945 (x  1.09) (total: 2.3 MB)  | 228829 (x  1.55) (total: 1.6 MB)        |

## Prod Traces (anonymized)

| PROTO MSG SIZE           | OTLP ZSTD - MEAN          | OTLP DICT ZSTD+STREAM MODE - MEAN | OTel ARROW ZSTD+STREAM MODE - MEAN   | OTLP DICT ZSTD+UNARY RPC - MEAN   | OTel  ARROW ZSTD+UNARY RPC MODE - MEAN   |
|--------------------------|---------------------------|-----------------------------------|--------------------------------------|-----------------------------------|------------------------------------------|
| **Uncompressed (bytes)** |                           |                                   |                                      |                                   |                                          |
| batch_size: 500          |   909780  (total: 757 MB) | 261413 (x  3.48) (total: 218 MB)  | 296585 (x  3.07) (total: 247 MB)     | 390731 (x  2.33) (total: 325 MB)  | 414941 (x  2.19) (total: 345 MB)         |
| batch_size: 5000         |  6354479  (total: 521 MB) | 2088389 (x  3.04) (total: 171 MB) | 2437692 (x  2.61) (total: 200 MB)    | 2504505 (x  2.54) (total: 205 MB) | 2867963 (x  2.22) (total: 235 MB)        |
| batch_size: 10000        | 11027424  (total: 441 MB) | 3825658 (x  2.88) (total: 153 MB) | 4590592 (x  2.40) (total: 184 MB)    | 4327249 (x  2.55) (total: 173 MB) | 5066534 (x  2.18) (total: 203 MB)        |
| **Compressed (bytes)**   |                           |                                   |                                      |                                   |                                          |
| batch_size: 500          |   213043  (total: 177 MB) | 89167 (x  2.39) (total: 74 MB)    | 86100 (x  2.47) (total: 72 MB)       | 217476 (x  0.98) (total: 181 MB)  | 200475 (x  1.06) (total: 167 MB)         |
| batch_size: 5000         |  1344735  (total: 110 MB) | 805998 (x  1.67) (total: 66 MB)   | 752228 (x  1.79) (total: 62 MB)      | 1197737 (x  1.12) (total: 98 MB)  | 1219833 (x  1.10) (total: 100 MB)        |
| batch_size: 10000        |  2376598  (total: 95 MB)  | 1542975 (x  1.54) (total: 62 MB)  | 1456160 (x  1.63) (total: 58 MB)     | 2006944 (x  1.18) (total: 80 MB)  | 2052184 (x  1.16) (total: 82 MB)         |

## Generated Traces

| PROTO MSG SIZE           | OTLP ZSTD - MEAN          | OTLP DICT ZSTD+STREAM MODE - MEAN  | OTel ARROW ZSTD+STREAM MODE - MEAN | OTLP DICT ZSTD+UNARY RPC - MEAN    | OTel  ARROW ZSTD+UNARY RPC MODE - MEAN |
|--------------------------|---------------------------|------------------------------------|------------------------------------|------------------------------------|----------------------------------------|
| **Uncompressed (bytes)** |                           |                                    |                                    |                                    |                                        |
| batch_size: 500          | 1643716  (total: 326 MB)  |   942916 (x  1.74) (total: 187 MB) | 726958 (x  2.26) (total: 144 MB)   | 943391 (x  1.74) (total: 187 MB)   | 722997 (x  2.27) (total: 143 MB)       |
| batch_size: 5000         | 16441551  (total: 296 MB) |  9431324 (x  1.74) (total: 170 MB) | 7257327 (x  2.27) (total: 131 MB)  | 9431799 (x  1.74) (total: 170 MB)  | 7094210 (x  2.32) (total: 128 MB)      |
| batch_size: 10000        | 32881741  (total: 263 MB) | 18860586 (x  1.74) (total: 151 MB) | 14574547 (x  2.26) (total: 117 MB) | 18861061 (x  1.74) (total: 151 MB) | 14172670 (x  2.32) (total: 113 MB)     |
| **Compressed (bytes)**   |                           |                                    |                                    |                                    |                                        |
| batch_size: 500          | 257561  (total: 51 MB)    |   197605 (x  1.30) (total: 39 MB)  | 96309 (x  2.67) (total: 19 MB)     | 197993 (x  1.30) (total: 39 MB)    | 98046 (x  2.63) (total: 19 MB)         |
| batch_size: 5000         | 2576491  (total: 46 MB)   |  1979197 (x  1.30) (total: 36 MB)  | 969131 (x  2.66) (total: 17 MB)    | 1979305 (x  1.30) (total: 36 MB)   | 977437 (x  2.64) (total: 18 MB)        |
| batch_size: 10000        | 5151447  (total: 41 MB)   |  3959998 (x  1.30) (total: 32 MB)  | 1965011 (x  2.62) (total: 16 MB)   | 3959419 (x  1.30) (total: 32 MB)   | 1979228 (x  2.60) (total: 16 MB)       |
