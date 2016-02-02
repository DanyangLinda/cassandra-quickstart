# Simple Cassandra application in Python

A simple "quick start" application showing how to connect to a Cassandra cluster in Python, create a keyspace and column family, insert and select data.

These instructions assume you are hosted with [Instaclustr](https://www.instaclustr.com), however this application will work with any Cassandra cluster if you provide your own node IPs and authentication. 

For instaclustr users, please read the [getting started](https://support.instaclustr.com/hc/en-us/articles/203759250-Connecting-to-a-Cluster
) guide on the Instaclustr support portal.


## Installation

`git clone https://github.com/instaclustr/cassandra-quickstart.git` 

## Setup and Configuration

Before you begin, check your [Cluster Details](https://www.instaclustr.com/dashboard/clusters) page to ensure your cluster has finished provisioning. All nodes and overall cluster status should be in RUNNING state.  

### configuration.json

Edit `configuration.json` with your cluster connection information:

|  | Instaclustr users | Non-Instaclustr users|
| ------------ | ------------- | ------------ |
| Contact points | Cluster details or Connection Details pages | `nodetool status`  |
| username and password | `iccassandra` user password can be found on Connection Details page | username: `cassandra` password: `cassandra` |
| Data centre name | Connection Details page | cassandra-rackdc.properties|

Tip: Access all cluster connection details from the [console](https://www.instaclustr.com/dashboard/clusters) by selecting `Connection Details` from the `Manage Cluster` dropdown on the clusters page. 


## Usage
`python cassandra-quickstart.py`

	
## Credits

[engineering@instaclustr.com](mailto:engineering@instaclustr.com)
