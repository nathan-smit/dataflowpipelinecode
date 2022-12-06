package com.pepkorit.cloud.solutions.realtimeredisupdate.pipeline;

import com.google.common.flogger.FluentLogger;
import java.util.Map;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

public class AddSkuDataToMessage extends DoFn<BranchCompanySkuTransactionValue, BranchCompanySkuTransactionValue> {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final PCollectionView<Map<String, String>> skus;

    public AddSkuDataToMessage(PCollectionView<Map<String, String>> skus) {
        this.skus = skus;
    }

    private Map<String, String> skuTable;

    @ProcessElement
    public void ProcessElement(ProcessContext c) {
        skuTable = c.sideInput(skus);
        String sku = c.element().getSku();

        logger.atInfo().log("Value for sku is " + sku);
        logger.atInfo().log("Found Value for sku is " + skuTable.get(sku));

        // check if the token is a key in the "branches" side input
        if (skuTable.containsKey(sku)) {
            c.output(
                    new BranchCompanySkuTransactionValue(
                            c.element().getBranch(),
                            c.element().getCompany(),
                            sku,
                            c.element().getTransactionId(),
                            c.element().getTable(),
                            c.element().getOp(),
                            c.element().getTs_ms(),
                            c.element().getValue()
                    )
            );
        }


    }
}