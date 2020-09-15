package ru.yandex.spark.yt.format

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.annotation.tailrec

class Lca extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  case class Node(left: Node, right: Node, id: Int) {
    var parent: Node = _
  }

  def parent(left: Node, right: Node, id: Int): Node = {
    val res = Node(left, right, id)
    left.parent = res
    right.parent = res
    res
  }

  "Lca" should "work" in {
    def lca(p: Node, q: Node): Node = {
      var pPointer = p
      var qPointer = q

      var pPointerDepth = Iterator.iterate(p)(_.parent).takeWhile(_ != null).length - 1
      var qPointerDepth = Iterator.iterate(q)(_.parent).takeWhile(_ != null).length - 1

      while (pPointer != qPointer) {
        if (pPointerDepth == qPointerDepth) {
          pPointer = pPointer.parent
          qPointer = qPointer.parent
        } else if (pPointerDepth > qPointerDepth) {
          pPointer = pPointer.parent
          pPointerDepth -= 1
        } else {
          qPointer = qPointer.parent
          qPointerDepth -= 1
        }
      }

      pPointer
    }

    test(lca)
  }

  it should "be more functional style" in {
    def lca(p: Node, q: Node): Node = {
      @tailrec
      def goUp(pPointer: Node, qPointer: Node, pPointerDepth: Int, qPointerDepth: Int): Node = {
        if (pPointer == qPointer) {
          pPointer
        } else {
          if (pPointerDepth == qPointerDepth) {
            goUp(pPointer.parent, qPointer.parent, pPointerDepth - 1, qPointerDepth - 1)
          } else if (pPointerDepth > qPointerDepth) {
            goUp(pPointer.parent, qPointer, pPointerDepth - 1, qPointerDepth)
          } else {
            goUp(pPointer, qPointer.parent, pPointerDepth, qPointerDepth - 1)
          }
        }
      }

      def countDepth(node: Node): Int = {
        Iterator.iterate(node)(_.parent).takeWhile(_ != null).length - 1
      }

      goUp(p, q, countDepth(p), countDepth(q))
    }

    test(lca)
  }

  it should "reverse list" in {
    def reverse[T](list: List[T]): List[T] = {
      @tailrec
      def inner(current: List[T], res: List[T]): List[T] = {
        current match {
          case Nil => res
          case head :: tail => inner(tail, head +: res)
        }
      }

      inner(list, Nil)
    }

    val table = Table(
      ("list", "expected"),
      (Nil, Nil),
      (List(1), List(1)),
      ((1 to 10).toList, 10.to(1, -1).toList)
    )

    forAll(table){(list: List[Int], expected: List[Int]) =>
      reverse(list) should contain theSameElementsInOrderAs expected
      reverse(reverse(list)) should contain theSameElementsInOrderAs list
    }
  }

  it should "reverse list inplace" in {
    case class ListNode(var next: ListNode, i: Int)

    def reverse(list: ListNode): ListNode = {
      var prev: ListNode = null
      var current = list
      while (current != null) {
        val next = current.next
        current.next = prev
        prev = current
        current = next
      }
      prev
    }

    reverse(null) shouldEqual null
    reverse(ListNode(null, 1)) shouldEqual ListNode(null, 1)
    reverse(ListNode(ListNode(null, 1), 2)) shouldEqual ListNode(ListNode(null, 2), 1)
    reverse(ListNode(ListNode(ListNode(null, 1), 2), 3)) shouldEqual ListNode(ListNode(ListNode(null, 3), 2), 1)
  }

  def test(lca: (Node, Node) => Node): Unit = {
    val node9 = Node(null, null, 9)
    val node8 = Node(null, null, 8)
    val node7 = Node(null, null, 7)
    val node6 = Node(null, null, 6)
    val node5 = parent(node8, node9, 5)
    val node4 = Node(null, null, 4)
    val node3 = parent(node6, node7, 3)
    val node2 = parent(node4, node5, 2)
    val node1 = parent(node2, node3, 1)

    lca(node4, node8) shouldEqual node2
    lca(node1, node7) shouldEqual node1
    lca(node9, node6) shouldEqual node1

    for (node <- Seq(node1, node2, node3, node4, node5, node6, node7, node8, node9)) {
      lca(node, node) shouldEqual node
    }
  }


}
