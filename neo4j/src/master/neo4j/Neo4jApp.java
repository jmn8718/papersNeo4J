package master.neo4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;


public class Neo4jApp {
	private static enum RelTypes implements RelationshipType {
		HAS, WROTE, REVIEWED, IS_FRIEND
	}
	
	private static final String JOURNAL = "Journal";
	private static final String CONFERENCE = "Conference";
	private static final String PAPER = "Paper";
	private static final String AUTHOR = "Author";
	private static final String REVIEWER = "Reviewer";

	private GraphDatabaseService graphDb;
	private String DB_PATH;
	private ExecutionEngine engine;
	private ExecutionResult result;
	
	private BufferedReader reader;
	
	public Neo4jApp(String path){
		this.DB_PATH = path;
	}
	
	public void connect(){
		System.out.println("Connecting......");
		this.graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		this.engine = new ExecutionEngine(graphDb);
		System.out.println("CONNECTED");
	}
	
	public void disconnect(){
		System.out.println("Disconnecting......");
		this.graphDb.shutdown();
		System.out.println("DISCONNECTED");
	}
	
	private Node addNode(Label label, HashMap<String, Object> properties){
		Node resul = null;
		try (Transaction tx = this.graphDb.beginTx()){
			Node userNode = this.graphDb.createNode(label);
			for (String nameProp : properties.keySet()) {
				userNode.setProperty(nameProp, properties.get(nameProp));
				//System.out.println(userNode.getProperty(nameProp));
			}
			resul = userNode;
			System.out.println("Node created ----- "+label.toString());
			tx.success();
			tx.close();
		}
		return resul;
	}
	
	private Node createConference(Label label, String name, String year, String city){
		Node conference = null;
		String query = "match (n:"+label.toString()+" {name:\""+name+"\", year:\""+year+"\", city:\""+city+"\"}) return n";
		if((conference = this.getNode(query))==null){
			HashMap<String,Object> properties = new HashMap<String, Object>();
			properties.put("name", name);
			properties.put("year", year);
			properties.put("city", city);
			conference = this.addNode(label, properties);
		} else
			System.out.println("Existe el nodo "+label.toString()+" : "+name+" , "+year+" , "+city);
		return conference;
	}
	
	private Node createJournal(Label label, String name, String volume){
		Node journal = null;
		String query = "match (n:"+label.toString()+" {name:\""+name+"\", volume:\""+volume+"\"}) return n";
		if((journal = this.getNode(query))==null){
			HashMap<String,Object> properties = new HashMap<String, Object>();
			properties.put("name", name);
			properties.put("volume", volume);
			journal = this.addNode(label, properties);
		} else
			System.out.println("Existe el nodo "+label.toString()+" : "+name+" , "+volume);
		return journal;
	}
	
	private Node createPaper(Label label, String title){
		Node paper = null;
		String query = "match (n:"+label.toString()+" {title:\""+title+"\"}) return n";
		if((paper = this.getNode(query))==null){
			HashMap<String,Object> properties = new HashMap<String, Object>();
			properties.put("title", title);
			paper = this.addNode(label, properties);
		} else
			System.out.println("Existe el nodo "+label.toString()+" : "+title);
		return paper;
	}
	
	private Node getNode(String query){
		Node node = null;
		try ( Transaction tx = graphDb.beginTx() ){
			System.out.println("QUERY: "+query);
			ExecutionResult result = engine.execute(query);
			if(result.hasNext()) {
				Iterator<Node> n_column = result.columnAs("n");
				for(Node n : IteratorUtil.asIterable(n_column)){
					node = n;
				}
			} else System.out.println(query+"\nConsulta vacia");
		}
		return node;
	}
	
	private Node createPerson(Label label, String surname){
		Node person = null;
		String query = "match (n:"+label.toString()+" {surname:\""+surname+"\"}) return n";
		if((person = this.getNode(query))==null){
			HashMap<String,Object> properties = new HashMap<String, Object>();
			properties.put("surname", surname);
			person = this.addNode(label, properties);
		} else
			System.out.println("Existe el nodo "+label.toString()+" : "+surname);
		return person;
	}
	
	private void createRelation(RelTypes rel, Node node1, Node node2){
		if(node1!=null && node2!=null){
			try ( Transaction tx = graphDb.beginTx() )
			{
				Relationship relationship = node1.createRelationshipTo( node2, rel );
				tx.success();
				tx.close();
			}
		} else 
			System.out.println("******************** no se ha podido crear la relación porque uno de los nodos no existe");
	}
	
	private void populateConferences(String file){
		System.out.println("/////////// POPULATING CONFERENCES");
		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(file)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String line;
		try {
			line = reader.readLine(); //para leer la cabecera
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Label labelAuthor = DynamicLabel.label(AUTHOR);
		Label labelConference = DynamicLabel.label(CONFERENCE);
		Label labelReviewer = DynamicLabel.label(REVIEWER);
		Label labelPaper = DynamicLabel.label(PAPER);
		Node author, conference, reviewer, paper;
		try {
			while((line = reader.readLine())!=null){
				String [] splits = line.split(",");
				paper = this.createPaper(labelPaper, splits[0]);
				System.out.println("conferenceName: "+splits[2]+" | year: "+splits[3]+" | city: "+splits[4]);
				conference = this.createConference(labelConference,splits[2], splits[3], splits[4]);
				this.createRelation(RelTypes.HAS, conference, paper);
				System.out.println("reviewer: "+splits[5]);
				reviewer = this.createPerson(labelReviewer, splits[5]);
				this.createRelation(RelTypes.REVIEWED, reviewer, paper);
				String [] authors = splits[1].split(";");
				System.out.println("authors: ");
				for (String aut : authors) {
					System.out.println(" | -> "+aut.trim());
					author = this.createPerson(labelAuthor, aut.trim());
					this.createRelation(RelTypes.WROTE, author, paper);
				}
				System.out.println("---------------------");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("//////////////////// POPULATING CONFERENCES COMPLETE");		
	}
	
	private void populateJournals(String file){
		System.out.println("/////////// POPULATING JOURNALS");
		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(file)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String line;
		try {
			line = reader.readLine(); //para leer la cabecera
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Label labelAuthor = DynamicLabel.label(AUTHOR);
		Label labelJournal = DynamicLabel.label(JOURNAL);
		Label labelPaper = DynamicLabel.label(PAPER);
		Label labelReviewer = DynamicLabel.label(REVIEWER);
		Node paper, author, journal, reviewer;
		try {
			while((line = reader.readLine())!=null){
				String [] splits = line.split(",");
				paper = this.createPaper(labelPaper, splits[0]);

				System.out.println("journalName: "+splits[2]+" | volume: "+splits[3]);
				journal = this.createJournal(labelJournal, splits[2], splits[3]);
				this.createRelation(RelTypes.HAS, journal, paper);
				String [] authors = splits[1].split(";");
				System.out.println("authors: ");
				for (String aut : authors) {
					System.out.println(" | -> "+aut.trim());
					author = this.createPerson(labelAuthor, aut.trim());
					this.createRelation(RelTypes.WROTE, author, paper);
				}
				System.out.println("reviewer: "+splits[4].trim());
				reviewer = this.createPerson(labelReviewer, splits[4].trim());
				this.createRelation(RelTypes.REVIEWED, reviewer, paper);
				System.out.println("---------------------");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("//////////////////// POPULATING JOURNALS COMPLETE");
	}
	
	private void populateFriendships(String file){
		System.out.println("/////////// POPULATING FRIENDSHIP");
		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(file)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String line;
		try {
			line = reader.readLine(); //para leer la cabecera
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Label labelAuthor = DynamicLabel.label(AUTHOR);
		Label labelReviewer = DynamicLabel.label(REVIEWER);
		Node author, reviewer;
		try {
			while((line = reader.readLine())!=null && line.length()>0){
				String [] splits = line.split(",");
				System.out.println("reviewer: "+splits[0].trim());
				reviewer = this.createPerson(labelReviewer, splits[0].trim());
				System.out.println("author: "+splits[1].trim());
				author = this.createPerson(labelAuthor, splits[1].trim());
				this.createRelation(RelTypes.IS_FRIEND, author, reviewer);
				System.out.println("---------------------");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("//////////////////// POPULATING FRIENDSHIP COMPLETE");
	}
		
	public void populate(String path){
		System.out.println("********************** POPULANDO");
		this.populateConferences(path+"conferences.csv");
		this.populateJournals(path+"journals.csv");
		this.populateFriendships(path+"friendships.csv");
		System.out.println("********************** POPULANDO FINALIZADO");
	}
	
	public void runQ1(String paperName){
		String query = "Match (p:Paper {title:\""+paperName+"\"})<-[:WROTE]-(a:Author),"
				+ "(p)<-[:REVIEWED]-(r:Reviewer) "
				+ "return a.surname AS author, r.surname AS reviewer";
		String out = paperName+",(";
		try ( Transaction tx = graphDb.beginTx() ){
			System.out.println("QUERY: "+query);
			this.result = engine.execute(query);
			if(result.hasNext()) {
				Iterator<Entry<String, Object>> iterator = null;
				String r = null;
				for ( Map<String, Object> row : result ){
					Set<Entry<String, Object>> entrySet = row.entrySet();
					iterator = entrySet.iterator();
					out+=(iterator.next().getValue().toString()+",");
					r = iterator.next().getValue().toString();
				}
				out=out.substring(0, out.length()-1)+"),"+r;
			} else {
					out+=")";
			}
			System.out.println(out);
			tx.success();
			tx.close();
		}
		this.escribirEnFichero("Q1: "+out);
	}
	
	public void runQ2(String conferenceName){
		String query = "MATCH (c:Conference {name:\""+conferenceName+"\"})-[:HAS]->(p:Paper)"
				+ "RETURN p.title";
		String out = conferenceName+",(";
		try ( Transaction tx = graphDb.beginTx() ){
			System.out.println("QUERY: "+query);
			this.result = engine.execute(query);
			if(result.hasNext()) {
				Iterator<Entry<String, Object>> iterator = null;
				for ( Map<String, Object> row : result ){
					Set<Entry<String, Object>> entrySet = row.entrySet();
					iterator = entrySet.iterator();
					out+=(iterator.next().getValue().toString()+",");
				}
				out=out.substring(0, out.length()-1)+")";
			} else {
					out+=")";
			}
			System.out.println(out);
			
			tx.success();
			tx.close();
		}
		this.escribirEnFichero("Q2: "+out);
	}

	public void runQ3(String authorName){
		String query = "MATCH (a:Author {surname:\""+authorName+"\"})-[:WROTE]->(p:Paper) "
				+ "RETURN p.title";
		String out = authorName+",(";
		try ( Transaction tx = graphDb.beginTx() ){
			System.out.println("QUERY: "+query);
			this.result = engine.execute(query);
			if(result.hasNext()) {
				Iterator<Entry<String, Object>> iterator = null;
				for ( Map<String, Object> row : result ){
					Set<Entry<String, Object>> entrySet = row.entrySet();
					iterator = entrySet.iterator();
					out+=(iterator.next().getValue().toString()+",");
				}
				out=out.substring(0, out.length()-1)+")";
			} else {
					out+=")";
			}
			System.out.println(out);
			
			tx.success();
			tx.close();
		}
		this.escribirEnFichero("Q3: "+out);
	}
	
	public void runQ4(String journalName, int journalVolume){
		String query = "MATCH (:Journal {name:\""+journalName+"\", volume:\""+journalVolume+"\"})-[:HAS]->(p:Paper)<-[:WROTE]-(a:Author),"
				+ "(p)<-[:REVIEWED]-(r:Reviewer),(a)-[:IS_FRIEND]->(r) "
				//+ "RETURN p.title as paper ,a.surname AS author,r.surname AS reviewer";
				+ "RETURN distinct p.title as paper ,a.surname AS author,r.surname AS reviewer";
		try ( Transaction tx = graphDb.beginTx() ){
			System.out.println("QUERY: "+query);
			this.result = engine.execute(query);
			if(result.hasNext()) {
				String out = new String();
				Iterator<Entry<String, Object>> iterator = null;
				for ( Map<String, Object> row : result ){
					Set<Entry<String, Object>> entrySet = row.entrySet();
					iterator = entrySet.iterator();
					out = iterator.next().getValue().toString()+",";
					out+=iterator.next().getValue().toString()+","; //sacamos el autor
					out+=iterator.next().getValue().toString(); //sacamos el reviewer
					System.out.println(out);
					this.escribirEnFichero("Q4: "+out);
				}
			} else {
				this.escribirEnFichero("Q4: ,,");
			}
			
			tx.success();
			tx.close();
		}
	}
	
	private void escribirEnFichero(String out){
		try {
			Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("JoseMiguelNavarro.log",true)));
			writer.append(out+"\n");
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String pathDB = "/home/motoko/Programas/neo4j-enterprise-2.2.0-M02/data/graph.db";
		String pathFolder = "files/";
		Neo4jApp app = new Neo4jApp(pathDB);
		app.connect();
		app.populate(pathFolder);
		
		app.runQ1("Transactional failure recovery for a distributed key-value store");
		app.runQ1("paperXX");
		
		app.runQ2("10KM_Laredo");
		app.runQ2("SRDS");
		
		app.runQ3("Gurtov");
		app.runQ3("Feng");
		app.runQ3("Soriente");
		
		app.runQ4("IEEE Network",27);
		app.runQ4("KSII",8);
		app.disconnect();
	}

}
