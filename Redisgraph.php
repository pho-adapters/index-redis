<?php

/*
 * This file is part of the Pho package.
 *
 * (c) Emre Sokullu <emre@phonetworks.org>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
 
namespace Pho\Kernel\Services\Index\Adapters;

use Pho\Kernel\Kernel;
use Pho\Kernel\Services\ServiceInterface;
use Pho\Kernel\Services\Index\IndexInterface;
use Pho\Lib\Graph\EntityInterface;
use GraphAware\Neo4j\Client\ClientBuilder;

/**
 * Neo4j indexing adapter
 * 
 * Bolt mode of connection is recommended. Bolt is stateful and binary, 
 * hence more efficient than HTTP or HTTPS.
 *
 * @author Emre Sokullu
 */
class Neo4j implements IndexInterface, ServiceInterface
{

     /**
     * Pho-kernel 
     * @var Kernel
     */
    protected $kernel;

    /**
     * Neo4J Client
     * @var \GraphAware\Neo4j\Client\Client
     */
    protected $client;


    /**
     * Constructor.
     * 
     * Initializes neo4j connection. Runs indexing on kernel signals.
     * 
     * @param Kernel $kernel Pho Kernel
     * @param string $uri Connection details for the Neo4J server
     */
    public function __construct(Kernel $kernel, string $uri = "")
    {
        $this->kernel = $kernel;
     
        $params = parse_url($uri);
        $this->client = ClientBuilder::create()
            ->addConnection($params["scheme"], $uri) 
            ->build();
        
        $this->subscribeGraphsystem();
    }

    /**
     * Listener for kernel events
     * 
     * Interfaces graphsystem and indexes
     * in every touch or delete operation.
     * 
     * @return void
     */
    protected function subscribeGraphsystem(): void
    {
        $this->kernel->events()->on('graphsystem.touched',  
            function(array $var) {
                $this->index($var);
            })
            ->on('graphsystem.node_deleted',  
            function(string $id) {
                $this->nodeDeleted($id);
            })
            ->on('graphsystem.edge_deleted',  
            function(string $id) {
                $this->edgeDeleted($id);
            }
        );
    }

    /**
     * {@inheritDoc}
     */
    public function query(string $query, array $params = array()): \Pho\Kernel\Services\Index\QueryResult
    {
        $result = $this->client->run($query, $params);
        $qr = new QueryResult($result);
        return $qr;
    }
 
 
   /**
    * {@inheritDoc}
    */
   public function checkNodeUniqueness(string $field_name, /*mixed*/ $field_value, string $label = ""): bool
   {
      if(!empty($label))
        $label = sprintf(":%s", $label);
      $cypher = sprintf("MATCH(n%s {%s: {%s}}) RETURN n", $label, $field_name, $field_name);
      $res = $this->query($cypher, [$field_name => $field_value]);   
      return (count($res->results()) == 0); // that means it's unique
   }

    /**
     * Direct access to the Neo4J client
     * 
     * This class does also provide direct read-only access to the 
     * client, for debugging purposes.
     *
     * @return \GraphAware\Neo4j\Client\Client
     */
    public function client(): \GraphAware\Neo4j\Client\Client
    {
        return $this->client;
    }

    /**
     * {@inheritDoc}
     */
    public function index(array $entity): void 
    {
        $this->kernel->logger()->info("Index request received by %s, a %s.", $entity["id"], $entity["label"]);
        $header = (int) hexdec($entity["id"][0]);
        if($header>0 && $header<6) /// node
        {
            $this->indexNode($entity);
        }
        elseif($header>=6 &&  $header < 11 )  // edge
        {
            $this->indexEdge($entity);
        }
        else {
            throw new \Exception(sprintf("Unrecognized entity type with header %s", $entity["id"][0]));
        }
        $this->kernel->logger()->info("Moving on");
    }

    /**
     * Indexes a node
     *
     * @param array $entity In array form.
     * 
     * @return void
     */
    protected function indexNode(array $entity): void
    {
        //$this->kernel->logger()->info("Header qualifies it to be indexed");
        $entity["attributes"]["udid"] = $entity["id"];
        $cq = sprintf("MERGE (n:%s {udid: {udid}}) SET n = {data}", $entity["label"]);
        $this->kernel->logger()->info(
            "The query will be as follows; %s with data ", 
            $cq
        //    print_r($entity["attributes"], true)
        );
        $result = $this->client->run($cq, [
            "udid" => $entity["id"],
            "data" => $entity["attributes"]
        ]);
    }

    /**
     * Indexes an edge
     *
     * @param array $entity In array form.
     * 
     * @return void
     */
    protected function indexEdge(array $entity): void
    {
        //$tail_id = $entity[]
        $entity["attributes"]["udid"] = $entity["id"];
        $cq = sprintf("MATCH(t {udid: {tail}}), (h {udid: {head}}) MERGE (t)-[e:%s {udid: {udid}}]->(h) SET e = {data}", $entity["label"]);
        $result = $this->client->run($cq, [
            "tail" => $entity["tail"],
            "head" => $entity["head"],
            "udid" => $entity["id"],
            "data" => $entity["attributes"]
        ]);
    }

    /**
     * {@inheritDoc}
     */
    public function nodeDeleted(string $id): void 
    {
        $this->kernel->logger()->info("Node deletion request received by %s.", $id);
        $cq = "MATCH (n {udid: {udid}}) OPTIONAL MATCH (n)-[e]-()  DELETE e, n";
        $this->client->run($cq, ["udid"=>$id]);
        $this->kernel->logger()->info("Node deleted. Moving on.");
    }

    /**
     * {@inheritDoc}
     */
    public function edgeDeleted(string $id): void
    {
        $this->kernel->logger()->info("Edge deletion request received by %s.", $id);
        $cq = "MATCH ()-[e {udid: {udid}}]-()  DELETE e";
        $this->client->run($cq, ["udid"=>$id]);
        $this->kernel->logger()->info("Edge deleted. Moving on.");
    }

    /**
     * {@inheritDoc}
     */
    public function flush(): void
    {
        $cq = "MATCH (n) OPTIONAL MATCH (n)-[e]-() DELETE e, n;";
        $this->client->run($cq);
    }

    public function createIndex(string $label, string $field_name): void
    {
       $cq = sprintf("CREATE INDEX ON :%s(%s)", $label, $field_name);
       $this->client->run($cq);
    }
 

}
