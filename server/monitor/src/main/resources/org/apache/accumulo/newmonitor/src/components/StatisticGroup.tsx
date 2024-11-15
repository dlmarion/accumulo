import { Card, ListGroup } from 'react-bootstrap';

interface Statistic {
  label: string;
  value: number | string;
}

interface StatisticGroupProps {
  title: string;
  statistics: Statistic[];
}

/**
 * Component that displays a group of statistics within a card.
 *
 * @component
 * @param {Object} props - The component props.
 * @param {string} props.title - The title of the statistic group.
 * @param {Array} props.statistics - An array of statistics to display.
 * @param {string} props.statistics[].label - The label of the statistic.
 * @param {string | number} props.statistics[].value - The value of the statistic.
 * @returns {JSX.Element} A card component containing the statistics.
 */
function StatisticGroup({ title, statistics }: StatisticGroupProps): JSX.Element {
  return (
    <Card className="mb-3">
      <Card.Body>
        <Card.Title>{title}</Card.Title>
        <ListGroup variant="flush">
          {statistics.map((stat) => (
            <ListGroup.Item key={stat.label}>
              <strong>{stat.label}:</strong> {stat.value}
            </ListGroup.Item>
          ))}
        </ListGroup>
      </Card.Body>
    </Card>
  );
}

export default StatisticGroup;