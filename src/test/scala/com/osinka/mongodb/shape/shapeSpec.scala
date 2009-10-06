package com.osinka.mongodb.shape

import org.specs._
import org.specs.runner._
import com.mongodb._

import Preamble._
import Config._

class shapeTest extends JUnit4(shapeSpec) with Console
object shapeTestRunner extends ConsoleRunner(shapeSpec)

object shapeSpec extends Specification("Scala way Mongo shapes") {
    val CollName = "test"
    val Const = "John Doe"

    val mongo = new Mongo(Host, Port).getDB(Database)

    doAfter { mongo.dropDatabase }

    "Shaped collection" should {
        val dbColl = mongo.getCollection(CollName)

        doBefore { dbColl.drop; mongo.requestStart }
        doAfter  { mongo.requestDone; dbColl.drop }

        "retrieve objects / ord" in {
            dbColl save Map("name" -> Const)
            val coll = dbColl.of(OrdUser)
            coll must haveSuperClass[ShapedCollection[OrdUser]]
            coll.firstOption must beSome[OrdUser].which{x => x.name == Const && x.mongoOID != null && x.mongoNS == CollName}
        }
        "retrieve objects / case" in {
            dbColl save Map("name" -> Const)
            val coll = dbColl.of(CaseUser)
            coll must haveSuperClass[ShapedCollection[CaseUser]]
            coll.firstOption must beSome[CaseUser].which{x => x.name == Const && x.mongoOID != null && x.mongoNS == CollName}
        }
        "store objects / case" in {
            val coll = dbColl.of(CaseUser)
            coll += CaseUser(Const)
            coll.firstOption must beSome[CaseUser].which{x => x.name == Const && x.mongoOID != null && x.mongoNS == CollName}
        }
        "store objects / ord" in {
            val coll = dbColl.of(OrdUser)
            val u = new OrdUser
            u.name = Const

            val r = coll += u
            r must haveClass[OrdUser]
            r.name must be_==(u.name)
            r.mongoOID must notBeNull

            coll.firstOption must beSome[OrdUser].which{x =>
                x.name == Const &&
                x.mongoOID != null &&
                x.mongoOID == r.mongoOID &&
                x.mongoNS == CollName
            }
        }
        "store/retrieve complex objects" in {
            val coll = dbColl.of(ComplexType)
            val c = new ComplexType(CaseUser(Const))
            val r = coll += c
            r must haveClass[ComplexType]
            r.user must be_==(c.user)
            r.mongoOID must notBeNull

            coll.firstOption must beSome[ComplexType].which{x =>
                x.user == CaseUser(Const) &&
                x.mongoOID == r.mongoOID
            }
        }
    }
    "Query" should {
        val dbColl = mongo.getCollection(CollName)

        doBefore { dbColl.drop; mongo.requestStart }
        doAfter  { mongo.requestDone; dbColl.drop }

        "support skip/limit" in {
            val coll = dbColl of CaseUser
            val N = 50
            for {val obj <- Array.fromFunction(x => CaseUser("User"+x))(N) }
                coll << obj

            coll must haveSize(N)
            coll applied Query() must haveSuperClass[ShapedCollection[CaseUser]]
            coll applied (Query() take 1) must haveSize(1)
            coll applied (Query() drop 10 take 5) must haveSize(5)
            coll applied (Query() drop N-5 take 10) must haveSize(5)
        }
        "ignore different shape" in {
            (dbColl of CaseUser) << CaseUser("user")

            val cmplxColl = dbColl of ComplexType
            cmplxColl must beEmpty
            cmplxColl.elements.collect must beEmpty
        }
    }
}