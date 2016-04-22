package com.eneco.trading.kafka.connect.twitter

import org.scalamock.scalatest.proxy.MockFactory
import twitter4j.{Status, Twitter}
import twitter4j.auth.AccessToken

import scala.util.Success

class TestSimpleTwitterWriter extends TestTwitterBase with MockFactory  {
  val cons = "cons"
  val consSecret = "consSecret"
  val access = "access"
  val accessSecret = "accessSecret"
  val myStatus = "I tweet, ergo sum"
  val myStatusId = 1337L

  test("TwitterWriter.ctor properly initializes the Twitter4j client") {
    val twitterMock = mock[Twitter]
    twitterMock.expects('setOAuthConsumer)(cons, consSecret)
    twitterMock.expects('setOAuthAccessToken)(new AccessToken(access, accessSecret))

    new TwitterWriter(cons,consSecret,access,accessSecret,twitterMock)
  }

  test("TwitterWriter.updateStatus properly dispatches the status to the Twitter4j client") {
    val statusMock = mock[Status]
    statusMock.expects('getId)().returning(myStatusId)

    val twitterMock = mock[Twitter]
    twitterMock.expects('setOAuthConsumer)(cons, consSecret)
    twitterMock.expects('setOAuthAccessToken)(new AccessToken(access, accessSecret))
    twitterMock.expects('updateStatus)(myStatus).returning(statusMock)

    val r = new TwitterWriter(cons,consSecret,access,accessSecret,twitterMock).updateStatus(myStatus)
    r shouldEqual Success(myStatusId)
  }
}
