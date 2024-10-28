const AWS = require('aws-sdk');
const axios = require('axios');

const s3 = new AWS.S3();

// Configuration
const BUCKET_NAME = process.env.AWS_BUCKET_NAME;
const APIGW_DEPLOYMENTS = process.env.APIGW_DEPLOYMENTS;
const APIGW_DEPL_ENVS = process.env.APIGW_DEPL_ENVS;

const AIOPS_AUTH_EP = process.env.AIOPS_AUTH_EP;
const AIOPS_AUTH_EP_USER = process.env.AIOPS_AUTH_EP_USER;
const AIOPS_AUTH_EP_PW = process.env.AIOPS_AUTH_EP_PW;

const AIOPS_OBS_JOBNAME = process.env.AIOPS_OBS_JOBNAME;
const AIOPS_TOPO_EP = process.env.AIOPS_TOPO_EP;
const AIOPS_RESOURCES_EP = process.env.AIOPS_RESOURCES_EP;
const AIOPS_REFERENCES_EP = process.env.AIOPS_REFERENCES_EP;

// AIOps entity types
const OPCO_ENT_TYPE = 'opco';
const DEPLOYMENT_ENT_TYPE = 'deployment';
const ENVIRONMENT_ENT_TYPE = 'environment';
const VHOST_ENT_TYPE = 'host';
const PROXY_ENT_TYPE = 'application';
const TARGETSRV_ENT_TYPE = 'backend';

// AIops relation types
const OPCO_TO_DEPL_REL_TYPE = 'manages';
const DEPL_TO_ENV_REL_TYPE = 'runsOn';
const ENV_TO_VHOST_REL_YTPE = 'contains';
const ENV_TO_PROX_REL_TYPE = 'uses';
const PROXY_TO_TSERVER_REL_TYPE = 'runsOn';

let AIOPS_AUTH_TOKEN = '';

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

// function to get the Auth token
async function getAuthToken() {
    try {
        const response = await axios.post(
            AIOPS_AUTH_EP,
            {
                username: AIOPS_AUTH_EP_USER,
                api_key: AIOPS_AUTH_EP_PW
            },
            {
                headers: {
                    'Content-Type': 'application/json',
                }
            }
        );
        const token = response.data.token;
        return token;
    } catch (error) {
        console.error('Error getting AIOps authentication token:', error.response ? error.response.data : error.message);
    }
}
// function to fetch the OPCO topology elements
async function fetchTopologyData() {
    const url = `${AIOPS_TOPO_EP}?_field=uniqueId&_field=name&_type=${OPCO_ENT_TYPE}&_include_global_resources=false&_include_count=false&_include_status=false&_include_status_severity=false&_include_metadata=false&_return_composites=false`;

    try {
        const response = await axios.get(url, {
            headers: {
                'accept': 'application/json',
                'X-TenantID': 'cfd95b7e-3bc7-4006-a4a8-a73a79c71255',
                'Authorization': 'Bearer ' + AIOPS_AUTH_TOKEN
            }
        });

        if (response.data) {
            const opcoToUniqueIdMapping = {};
            response.data._items.forEach(item => {
                opcoToUniqueIdMapping[item.name] = item.uniqueId;
            });
            return opcoToUniqueIdMapping;
        }
        else {
            console.error('Error collecting OPCOs from AIOps topology!');
            return null;
        }

    } catch (error) {
        console.log(error);
        console.error('Error fetching topology data:', error.message);
        return null;
    }

}
// generic function to list objects from within an S3 bucket
async function listObjectsFromS3(prefix) {
    const params = {
        Bucket: BUCKET_NAME,
        Prefix: prefix
    };
    const data = await s3.listObjectsV2(params).promise();
    return data.Contents;
}

// generic function to receive an objects from within an S3 bucket
async function getObjectFromS3(key) {
    const params = {
        Bucket: BUCKET_NAME,
        Key: key
    };
    const data = await s3.getObject(params).promise();
    return JSON.parse(data.Body.toString('utf-8'));
}

// helper function to extract the opco name from the file name
async function extractLettersAfterDash(string) {
    const lastDashIndex = string.lastIndexOf('-');

    if (lastDashIndex !== -1 && lastDashIndex + 3 <= string.length) {
        const extractedLetters = string.substring(lastDashIndex + 1, lastDashIndex + 3);
        return extractedLetters.toUpperCase();
    } else {
        return null;
    }
}

// helper function to convert an epoch timestamp to a human readable format
async function epochToHumanReadable(epochSeconds) {
    const date = new Date(epochSeconds);

    const readable = new Intl.DateTimeFormat('de-DE', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false // Use 24-hour format
    }).format(date);
    return readable;
}

// helper function to post data to AIOps
async function sendToTopoApi(endpoint, data) {
    const headers = {
        'accept': 'application/json',
        'X-TenantID': 'cfd95b7e-3bc7-4006-a4a8-a73a79c71255',
        'JobId': AIOPS_OBS_JOBNAME,
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + AIOPS_AUTH_TOKEN
    };

    try {
        const response = await axios.post(endpoint, data, { headers });
        console.log(`Successfully sent data to topology API!`, response.status);
        return true;
    } catch (error) {
        console.log(error);
        console.error(`Error sending data to topology API!`, error.response ? error.response.data : error.message);
        return false;
    }
}

// function to process the proxy folders
async function processProxy(proxyFolder) {
    const proxies = [];
    const proxyObjects = await listObjectsFromS3(proxyFolder);

    for (const obj of proxyObjects) {
        if (obj.Key.endsWith('.json') && obj.Key.includes('target-servers')) {
            const targetServerData = await getObjectFromS3(obj.Key);
            proxies.push(targetServerData);
        }
    }

    return proxies;
}

// function to process the virtual hosts folders
async function processVirtualHosts(environmentFolder) {
    const virtualHosts = [];
    const virtualHostObjects = await listObjectsFromS3(`${environmentFolder}virtual-hosts/`);
    for (const obj of virtualHostObjects) {
        if (obj.Key.endsWith('.json')) {
            const virtualHostData = await getObjectFromS3(obj.Key);
            virtualHosts.push(virtualHostData);
        }
    }
    return virtualHosts;
}

// function to process the environment folders
async function processEnvironmentData(environmentFile) {
    const environmentData = await getObjectFromS3(environmentFile);

    // Extract properties from the "properties" array and convert them to direct attributes
    if (environmentData && environmentData.properties && environmentData.properties.property) {
        environmentData.properties.property.forEach(prop => {
            environmentData[prop.name] = prop.value;
        });
        delete environmentData.properties;
        return environmentData;
    }

    return null;
}

// function to process the deployment folders
async function processDeploymentData(deploymentFile) {
    const deploymentData = await getObjectFromS3(deploymentFile);

    if (deploymentData) {
        delete deploymentData.environments;
        return deploymentData;
    }

    return null;
}

// function to process the environment for a country
async function processCountry(deployment, environments) {
    const deploymentFile = `${deployment}/${deployment}.json`;  // Assuming the deployment file is always named after the deployment
    const deploymentDetails = await processDeploymentData(deploymentFile);

    const opcoName = await extractLettersAfterDash(deployment);
    const deploymentData = {
        opco: opcoName,
        deployment: deploymentDetails,
        environments: []
    };

    let environmentObject = {
        proxies: [],
        'virtual-hosts': []
    };

    for (const environment of environments) {
        const environmentFolder = `${deployment}/${environment}/`;
        const environmentFile = `${environmentFolder}${environment}.json`;  // Assuming the environment file is always named after the environment

        const environmentObjects = await listObjectsFromS3(environmentFolder);
        const environmentData = await processEnvironmentData(environmentFile);

        Object.assign(environmentObject, environmentData);

        // Process virtual hosts for the env
        const virtualHosts = await processVirtualHosts(environmentFolder);
        environmentObject['virtual-hosts'] = virtualHosts;

        // Process proxies and virtual hosts for the deployment
        for (const obj of environmentObjects) {
            if (obj.Key.endsWith('.json') && obj.Key.includes('proxies') && !obj.Key.includes('target-servers')) {
                const proxyData = await getObjectFromS3(obj.Key);
                const proxyFolder = obj.Key.replace(/[^/]+$/, 'target-servers/');

                const targetServers = await processProxy(proxyFolder);
                proxyData.targetServers = targetServers;

                environmentObject.proxies.push({
                    ...proxyData
                });
            }
        }
        deploymentData.environments.push(environmentObject);
    }
    return deploymentData;
}

async function processAllDeployments() {
    const results = [];
    const apigwDeploymentsList = APIGW_DEPLOYMENTS ? APIGW_DEPLOYMENTS.split(',') : [];
    const apigwDeployments = apigwDeploymentsList.map(item => item.trim());

    const apigwDeploymentsEnvList = APIGW_DEPL_ENVS ? APIGW_DEPL_ENVS.split(',') : [];
    const apigwDeploymentsEnvs = apigwDeploymentsEnvList.map(item => item.trim());

    for (const country of apigwDeployments) {
        const countryData = await processCountry(country, apigwDeploymentsEnvs);
        results.push(countryData);
    }
    return results;
}

// function to transform the JSON data to a format that AIOps can use
async function transformDataAndSendToApi(data) {
    if (data && data.length > 0) {
        console.log(`Extracted ${data.length} OPCO item(s)...`);

        // collect OPCOs from AIOps
        const opcoTopoData = await fetchTopologyData();
        if (!opcoTopoData) {
            console.error('No OCPO data found in AIOps! No topology will be send to AIOps!');
        }
        else {
            // loop over opcos
            for (const opcoElement of data) {
                const opcoName = opcoElement.opco;
                const opcoUniqueId = opcoTopoData[opcoName];

                const deploymentDetails = opcoElement.deployment;
                if (deploymentDetails && deploymentDetails.name) {
                    const deploymentName = deploymentDetails.name;
                    console.log(`Working on OPCO with name ${opcoName} and uniqueId ${opcoUniqueId} for deployment ${deploymentName}`);

                    // ============================================================
                    // create the deployment topology element
                    const deploymentUniqueName = `${opcoName}_${deploymentName}`;

                    let deploymentTopoElement = {
                        uniqueId: deploymentUniqueName,
                        entityTypes: [DEPLOYMENT_ENT_TYPE],
                        matchTokens: [deploymentUniqueName],
                        opco: opcoName,
                        tags: [deploymentName]
                    };
                    Object.assign(deploymentTopoElement, deploymentDetails);
                    delete deploymentTopoElement.properties;
                    if (deploymentTopoElement.createdAt) {
                        deploymentTopoElement.createdAt = await epochToHumanReadable(deploymentTopoElement.createdAt);
                    }
                    if (deploymentTopoElement.lastModifiedAt) {
                        deploymentTopoElement.lastModifiedAt = await epochToHumanReadable(deploymentTopoElement.lastModifiedAt);
                    }
                    if (await sendToTopoApi(AIOPS_RESOURCES_EP, deploymentTopoElement)) {
                        console.log(`Successfully sent data for deployment ${deploymentName} in OPCO ${opcoName}`);
                    }
                    else {
                        console.error(`Error sending data for deployment ${deploymentName} in OPCO ${opcoName}`);
                    }

                    // create the reference from the OPCO to the deployment
                    const deploymentTopoElementRelation = {
                        _fromUniqueId: opcoUniqueId,
                        _toUniqueId: deploymentUniqueName,
                        _edgeType: OPCO_TO_DEPL_REL_TYPE
                    }
                    if (await sendToTopoApi(AIOPS_REFERENCES_EP, deploymentTopoElementRelation)) {
                        console.log(`Successfully created relation from OPCO ${opcoName} to deployment ${deploymentName}`);
                    }
                    else {
                        console.error(`Error creating relation from OPCO ${opcoName} to deployment ${deploymentName}:`);
                    }

                    // ============================================================

                    // create the environment(s) for this deployment
                    const environmentsDetails = opcoElement.environments;
                    if (environmentsDetails && environmentsDetails.length > 0) {
                        for (const environment of environmentsDetails) {
                            const envName = environment.name;
                            const envUniqueId = `${opcoName}_${deploymentName}_${envName}`;
                            console.log(`Working on environment ${envName} for deployment ${deploymentName} in OPCO ${opcoName}...`);
                            let environmentTopoElement = {
                                uniqueId: envUniqueId,
                                entityTypes: [ENVIRONMENT_ENT_TYPE],
                                matchTokens: [envUniqueId],
                                opco: opcoName,
                                deployment: deploymentName,
                                tags: [deploymentName, envName]
                            };
                            Object.assign(environmentTopoElement, environment);
                            if (environmentTopoElement.createdAt) {
                                environmentTopoElement.createdAt = await epochToHumanReadable(environmentTopoElement.createdAt);
                            }
                            if (environmentTopoElement.lastModifiedAt) {
                                environmentTopoElement.lastModifiedAt = await epochToHumanReadable(environmentTopoElement.lastModifiedAt);
                            }

                            delete environmentTopoElement.proxies;
                            delete environmentTopoElement['virtual-hosts'];

                            if (await sendToTopoApi(AIOPS_RESOURCES_EP, environmentTopoElement)) {
                                console.log(`Successfully sent data for environment ${envName} of deployment ${deploymentName} in OPCO ${opcoName}`);
                            }
                            else {
                                console.error(`Error sending data for environment ${envName} of deployment ${deploymentName} in OPCO ${opcoName}`);
                            }

                            // create the reference from the deployment to the environment
                            const environmentTopoElementRelation = {
                                _fromUniqueId: deploymentUniqueName,
                                _toUniqueId: envUniqueId,
                                _edgeType: DEPL_TO_ENV_REL_TYPE
                            }
                            if (await sendToTopoApi(AIOPS_REFERENCES_EP, environmentTopoElementRelation)) {
                                console.log(`Successfully created relation from deployment ${deploymentName} to environment ${envName}`);
                            }
                            else {
                                console.error(`Error creating relation from deployment ${deploymentName} to environment ${envName}`);
                            }

                            // ============================================================

                            // create the virtual hosts
                            const virtualHosts = environment['virtual-hosts'];
                            if (virtualHosts && virtualHosts.length > 0) {
                                for (const virtualHost of virtualHosts) {
                                    const vHostName = virtualHost.name;
                                    const vHostUniqueId = `${opcoName}_${deploymentName}_${envName}_${vHostName}`;
                                    console.log(`Working on virtual host ${vHostName} for deployment ${deploymentName} in environment ${envName} in OPCO ${opcoName}...`);
                                    let vHostTopoElement = {
                                        uniqueId: vHostUniqueId,
                                        entityTypes: [VHOST_ENT_TYPE],
                                        matchTokens: [vHostUniqueId],
                                        opco: opcoName,
                                        environment: envName,
                                        deployment: deploymentName,
                                        tags: [deploymentName, envName, vHostName]
                                    };

                                    if (virtualHost && virtualHost.properties && virtualHost.properties.property) {
                                        virtualHost.properties.property.forEach(prop => {
                                            virtualHost[prop.name] = prop.value;
                                        });
                                        delete virtualHost.properties;
                                    }
                                    Object.assign(vHostTopoElement, virtualHost);

                                    if (await sendToTopoApi(AIOPS_RESOURCES_EP, vHostTopoElement)) {
                                        console.log(`Successfully sent data for virtual host ${vHostName} of deployment ${deploymentName} in environment ${envName} in OPCO ${opcoName}`);
                                    }
                                    else {
                                        console.error(`Error sending data for virtual host ${vHostName} of deployment ${deploymentName} in environment ${envName} in OPCO ${opcoName}`);
                                    }

                                    // create the reference from the environment to the virtual host
                                    const vHostTopoElementRelation = {
                                        _fromUniqueId: envUniqueId,
                                        _toUniqueId: vHostUniqueId,
                                        _edgeType: ENV_TO_VHOST_REL_YTPE
                                    }

                                    if (await sendToTopoApi(AIOPS_REFERENCES_EP, vHostTopoElementRelation)) {
                                        console.log(`Successfully created relation from environment ${envName} to virtual host ${vHostName}`);
                                    }
                                    else {
                                        console.error(`Error creating relation from environment ${envName} to virtual host ${vHostName}`);
                                    }
                                } // virtual hosts loop
                            }
                            else {
                                console.warn(`No virtual hosts found for for deployment ${deploymentName} in environment ${envName} of OPCO ${opcoName}!`);
                            }
                            // ============================================================
                            // create the proxies
                            const proxies = environment.proxies;
                            if (proxies && proxies.length > 0) {
                                for (const proxy of proxies) {
                                    const proxyName = proxy.name;
                                    const proxyUniqueId = `${opcoName}_${deploymentName}_${envName}_${proxyName}`;
                                    console.log(`Working on proxy ${proxyName} for deployment ${deploymentName} in environment ${envName} in OPCO ${opcoName}...`);
                                    let proxyTopoElement = {
                                        uniqueId: proxyUniqueId,
                                        entityTypes: [PROXY_ENT_TYPE],
                                        matchTokens: [proxyUniqueId],
                                        opco: opcoName,
                                        environment: envName,
                                        deployment: deploymentName,
                                        tags: [deploymentName, envName, proxyName]
                                    };

                                    Object.assign(proxyTopoElement, proxy);
                                    delete proxyTopoElement.targetServers;

                                    if (proxyTopoElement.createdAt) {
                                        proxyTopoElement.createdAt = await epochToHumanReadable(proxyTopoElement.createdAt);
                                    }
                                    if (proxyTopoElement.lastModifiedAt) {
                                        proxyTopoElement.lastModifiedAt = await epochToHumanReadable(proxyTopoElement.lastModifiedAt);
                                    }

                                    if (await sendToTopoApi(AIOPS_RESOURCES_EP, proxyTopoElement)) {
                                        console.log(`Successfully sent data for proxy ${proxyName} of deployment ${deploymentName} in environment ${envName} in OPCO ${opcoName}`);
                                    }
                                    else {
                                        console.error(`Error sending data for proxy ${proxyName} of deployment ${deploymentName} in environment ${envName} in OPCO ${opcoName}`);
                                    }

                                    // create the reference from the environment to the proxy
                                    const proxyTopoElementRelation = {
                                        _fromUniqueId: envUniqueId,
                                        _toUniqueId: proxyUniqueId,
                                        _edgeType: ENV_TO_PROX_REL_TYPE
                                    }
                                    if (await sendToTopoApi(AIOPS_REFERENCES_EP, proxyTopoElementRelation)) {
                                        console.log(`Successfully created relation from environment ${envName} to proxy ${proxyName}`);
                                    }
                                    else {
                                        console.error(`Error creating relation from environment ${envName} to proxy ${proxyName}`);
                                    }

                                    // create the target servers for each prox
                                    const targetServers = proxy.targetServers;
                                    if (targetServers && targetServers.length > 0) {
                                        for (const targetServer of targetServers) {
                                            const targetServerName = targetServer.name;
                                            const targetServerHost = targetServer.host;
                                            const targetServerUniqueId = `${opcoName}_${deploymentName}_${envName}_${targetServerHost}`;
                                            console.log(`Working on targetServer ${targetServerName} for proxy ${proxyName} for deployment ${deploymentName} in environment ${envName} in OPCO ${opcoName}...`);
                                            let targetServerTopoElement = {
                                                uniqueId: targetServerUniqueId,
                                                entityTypes: [TARGETSRV_ENT_TYPE],
                                                matchTokens: [targetServerUniqueId],
                                                opco: opcoName,
                                                environment: envName,
                                                deployment: deploymentName,
                                                proxy: proxyName,
                                                tags: [deploymentName, envName, targetServerName, proxyName]
                                            };

                                            Object.assign(targetServerTopoElement, targetServer);

                                            if (await sendToTopoApi(AIOPS_RESOURCES_EP, targetServerTopoElement)) {
                                                console.log(`Successfully sent data for targetServer ${targetServerName} of deployment ${deploymentName} in environment ${envName} in OPCO ${opcoName}`);
                                            }
                                            else {
                                                console.error(`Error sending data for targetServer ${targetServerName} of deployment ${deploymentName} in environment ${envName} in OPCO ${opcoName}`);
                                            }

                                            // create the reference from the proxy to the targetServer
                                            const targetServerTopoElementRelation = {
                                                _fromUniqueId: proxyUniqueId,
                                                _toUniqueId: targetServerUniqueId,
                                                _edgeType: PROXY_TO_TSERVER_REL_TYPE
                                            }
                                            if (await sendToTopoApi(AIOPS_REFERENCES_EP, targetServerTopoElementRelation)) {
                                                console.log(`Successfully created relation from proxy ${proxyName} to targetServer ${targetServerName}`);
                                            }
                                            else {
                                                console.error(`Error creating relation from proxy ${proxyName} to targetServer ${targetServerName}`);
                                            }
                                        }
                                    }
                                    else {
                                        console.warn(`No target servers found for proxy ${proxyName} in deployment ${deploymentName} in environment ${envName} of OPCO ${opcoName}!`);
                                    }

                                } // proxy loop
                            }
                            else {
                                console.warn(`No proxies found for deployment ${deploymentName} in environment ${envName} of OPCO ${opcoName}!`);
                            }
                            // ============================================================
                        } // environment loop
                    }
                    else {
                        console.error(`Error creating environment(s) for deployment ${deploymentName} in OPCO ${opcoName}:`, response.status);
                    }
                }
                else {
                    console.error(`No deployment found for OPCO with name ${opcoName} and uniqueId ${opcoUniqueId} !`);
                }
            }
        }
    }
    else {
        console.error('No data found in S3 bucket. Please see previous log messages for error details.');
    }
}

(async function main() {
    try {
        const data = await processAllDeployments();
        //console.log('Generated data array:', JSON.stringify(data, null, 2));
        AIOPS_AUTH_TOKEN = await getAuthToken();
        await transformDataAndSendToApi(data);
    } catch (error) {
        console.error('Error processing S3 data:', error);
    }
})();