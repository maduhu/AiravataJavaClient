package org.apache.airavata.client.EchoExperiment;

import org.apache.airavata.api.Airavata;
import org.apache.airavata.api.client.AiravataClientFactory;
import org.apache.airavata.model.appcatalog.appinterface.DataType;
import org.apache.airavata.model.appcatalog.appinterface.InputDataObjectType;
import org.apache.airavata.model.appcatalog.appinterface.OutputDataObjectType;
import org.apache.airavata.model.error.*;
import org.apache.airavata.model.util.ExperimentModelUtil;
import org.apache.airavata.model.workspace.experiment.ComputationalResourceScheduling;
import org.apache.airavata.model.workspace.experiment.ErrorDetails;
import org.apache.airavata.model.workspace.experiment.Experiment;
import org.apache.airavata.model.workspace.experiment.UserConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created by ruveni on 09/11/15.
 */
public class Test {
    private final static Logger logger = LoggerFactory.getLogger(Test.class);

    public static void main(String args[]) throws Exception{
        Airavata.Client airavataClient = AiravataClientFactory.createAiravataClient("127.0.0.1", 8930);

        String appId = "Echo_0d64f618-72ee-4cbb-babc-c47324543ffd";

        List<InputDataObjectType> exInputs = new ArrayList<InputDataObjectType>();
        InputDataObjectType input = new InputDataObjectType();
        input.setName("Input_to_Echo");
        input.setType(DataType.STRING);
        input.setValue("Echoed_Output=Hello World");
        exInputs.add(input);

        List<OutputDataObjectType> exOut = new ArrayList<OutputDataObjectType>();
        OutputDataObjectType output = new OutputDataObjectType();
        output.setName("Echoed_Output");
        output.setType(DataType.STRING);
        output.setValue("YYY");
        exOut.add(output);

        Experiment simpleExperiment = ExperimentModelUtil.createSimpleExperiment("default", "admin", "echoExperiment", "Echo Exp", appId, exInputs);
        simpleExperiment.setExperimentOutputs(exOut);

        Map<String,String> computeResources = airavataClient.getAvailableAppInterfaceComputeResources(appId);
        String id = computeResources.keySet().iterator().next();
        String resourceName = computeResources.get(id);
        System.out.println(computeResources.size());
        System.out.println(id);
        System.out.println(resourceName);
        ComputationalResourceScheduling scheduling = ExperimentModelUtil.createComputationResourceScheduling(id, 1, 1, 1, "normal", 30, 0, 1, "sds128");
        UserConfigurationData userConfigurationData = new UserConfigurationData();
        userConfigurationData.setAiravataAutoSchedule(false);
        userConfigurationData.setOverrideManualScheduledParams(false);
        userConfigurationData.setComputationalResourceScheduling(scheduling);
        simpleExperiment.setUserConfigurationData(userConfigurationData);

        String exp = airavataClient.createExperiment(simpleExperiment);
        System.out.println(exp);
        try{
            airavataClient.launchExperiment(exp,"sample");
        }catch (Exception e){
            e.printStackTrace();
        }
        getExperiment(airavataClient,exp);

    }
    public static void getExperiment(Airavata.Client client, String expId) throws Exception {
        try {
            Experiment experiment = client.getExperiment(expId);
            List<ErrorDetails> errors = experiment.getErrors();
            List<OutputDataObjectType> outputs=experiment.getExperimentOutputs();
            if (errors != null && !errors.isEmpty()) {
                for (ErrorDetails error : errors) {
                    System.out.println("ERROR MESSAGE : " + error.getActualErrorMessage());
                }
            }
            System.out.println(outputs.size());
            for(OutputDataObjectType output : outputs){
                System.out.println("Output: "+output.getName()+" "+output.getValue());
            }

        } catch (ExperimentNotFoundException e) {
            logger.error("Experiment does not exist", e);
            throw new ExperimentNotFoundException("Experiment does not exist");
        } catch (AiravataSystemException e) {
            logger.error("Error while retrieving experiment", e);
            throw new AiravataSystemException(AiravataErrorType.INTERNAL_ERROR);
        } catch (InvalidRequestException e) {
            logger.error("Error while retrieving experiment", e);
            throw new InvalidRequestException("Error while retrieving experiment");
        } catch (AiravataClientException e) {
            logger.error("Error while retrieving experiment", e);
            throw new AiravataClientException(AiravataErrorType.INTERNAL_ERROR);
        }
    }
}
