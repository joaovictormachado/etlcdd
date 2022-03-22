SELECT DATE (dt)`Data`,
       SUM(IF(forma="FIADO" AND (cd_cli NOT IN (639, 745, 781, 866, 236)),vl,0))`Crediário`, 
       SUM(IF(operacao="RECEBIMENTO",vl,0)`Recebimento`)
       SUM(IF(forma="CARTAO",vl,0))`Cartão`, 
       SUM(IF(operacao="VENDA" AND (forma="DINHEIRO" OR forma="TROCO"), vl, 0))`Dinheiro`,
       SUM(IF(operacao="SANGRIA",vl,0))`Sangria`,
       FROM mov_caixa GROUP BY DATE(dt) ORDER BY dt